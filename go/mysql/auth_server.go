/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	// MIXCHARS is the number of characters to use in the mix
	MIXCHARS = 32
	// SALTLENGTH is the length of the salt
	SALTLENGTH = 20
	// ITERATERMULNUM is the number of iterations to use
	ITERATERMULNUM = 1000 //nolint: revive
)

// AuthServer is the interface that servers must implement to validate
// users and passwords. It needs to be able to return a list of AuthMethod
// interfaces which implement the supported auth methods for the server.
type AuthServer interface {
	// AuthMethods returns a list of auth methods that are part of this
	// interface. Building an authentication server usually means
	// creating AuthMethod instances with the known helpers for the
	// currently supported AuthMethod implementations.
	//
	// When a client connects, the server checks the list of auth methods
	// available. If an auth method for the requested auth mechanism by the
	// client is available, it will be used immediately.
	//
	// If there is no overlap between the provided auth methods and
	// the one the client requests, the server will send back an
	// auth switch request using the first provided AuthMethod in this list.
	AuthMethods() []AuthMethod

	// DefaultAuthMethodDescription returns the auth method that the auth server
	// sends during the initial server handshake. This needs to be either
	// `mysql_native_password` or `caching_sha2_password` as those are the only
	// supported auth methods during the initial handshake.
	//
	// It's not needed to also support those methods in the AuthMethods(),
	// in fact, if you want to only support for example clear text passwords,
	// you must still return `mysql_native_password` or `caching_sha2_password`
	// here and the auth switch protocol will be used to switch to clear text.
	DefaultAuthMethodDescription() AuthMethodDescription
}

// AuthMethod interface for concrete auth method implementations.
// When building an auth server, you usually don't implement these yourself
// but the helper methods to build AuthMethod instances should be used.
type AuthMethod interface {
	// Name returns the auth method description for this implementation.
	// This is the name that is sent as the auth plugin name during the
	// Mysql authentication protocol handshake.
	Name() AuthMethodDescription

	// HandleUser verifies if the current auth method can authenticate
	// the given user with the current auth method. This can be useful
	// for example if you only have a plain text of hashed password
	// for specific users and not all users and auth method support
	// depends on what you have.
	HandleUser(conn *Conn, user string) bool

	// AllowClearTextWithoutTLS identifies if an auth method is allowed
	// on a plain text connection. This check is only enforced
	// if the listener has AllowClearTextWithoutTLS() disabled.
	AllowClearTextWithoutTLS() bool

	// AuthPluginData generates the information for the auth plugin.
	// This is included in for example the auth switch request. This
	// is auth plugin specific and opaque to the Mysql handshake
	// protocol.
	AuthPluginData() ([]byte, error)

	// HandleAuthPluginData handles the returned auth plugin data from
	// the client. The original data the server sent is also included
	// which can include things like the salt for `mysql_native_password`.
	//
	// The remote address is provided for plugins that also want to
	// do additional checks like IP based restrictions.
	HandleAuthPluginData(conn *Conn, user string, serverAuthPluginData []byte, clientAuthPluginData []byte, remoteAddr net.Addr) (Getter, error)
}

// UserValidator is an interface that allows checking if a specific
// user will work for an auth method. This interface is called by
// all the default helpers that create AuthMethod instances for
// the various supported Mysql authentication methods.
type UserValidator interface {
	HandleUser(user string, plugin string) bool
}

// CacheState is a state that is returned by the UserEntryWithCacheHash
// method from the CachingStorage interface. This state is needed to indicate
// whether the authentication is accepted, rejected by the cache itself
// or if the cache can't fullfill the request. In that case it indicates
// that with AuthNeedMoreData.
type CacheState int

const (
	// AuthRejected is used when the cache knows the request can be rejected.
	AuthRejected CacheState = iota
	// AuthAccepted is used when the cache knows the request can be accepted.
	AuthAccepted
	// AuthNeedMoreData is used when the cache doesn't know the answer and more data is needed.
	AuthNeedMoreData
)

// HashStorage describes an object that is suitable to retrieve user information
// based on the hashed authentication response for mysql_native_password.
//
// In general, an implementation of this would use an internally stored password
// that is hashed twice with SHA1.
//
// The VerifyHashedMysqlNativePassword helper method can be used to verify
// such a hash based on the salt and auth response provided here after retrieving
// the hashed password from the storage.
type HashStorage interface {
	UserEntryWithHash(conn *Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error)
}

// PlainTextStorage describes an object that is suitable to retrieve user information
// based on the plain text password of a user. This can be obtained through various
// Mysql authentication methods, such as `mysql_clear_passwrd`, `dialog` or
// `caching_sha2_password` in the full authentication handshake case of the latter.
//
// This mechanism also would allow for picking your own password storage in the backend,
// such as BCrypt, SCrypt, PBKDF2 or Argon2 once the plain text is obtained.
//
// When comparing plain text passwords directly, please ensure to use `subtle.ConstantTimeCompare`
// to prevent timing based attacks on the password.
type PlainTextStorage interface {
	UserEntryWithPassword(conn *Conn, user string, password string, remoteAddr net.Addr) (Getter, error)
}

// FullAuthStorage describes an object that is suitable to retrieve user information
// based on full authentication, specifically for the `caching_sha2_password` method.
// Full authentication is a more secure process, which includes steps such as
// sending the password, the server validating it, and sometimes requiring additional
// steps like RSA key pair-based password encryption.
//
// When implementing the full authentication process, ensure the use of secure
// practices to prevent various types of attacks, including timing-based attacks
// on the password, by using mechanisms like `subtle.ConstantTimeCompare`.
type FullAuthStorage interface {
	UserEntryWithFullAuth(conn *Conn, salt []byte, user string, password string, remoteAddr net.Addr) (Getter, error)
}

// CachingStorage describes an object that is suitable to retrieve user information
// based on a hashed value of the password. This applies to the `caching_sha2_password`
// authentication method.
//
// The cache would hash the password internally as `SHA256(SHA256(password))`.
//
// The VerifyHashedCachingSha2Password helper method can be used to verify
// such a hash based on the salt and auth response provided here after retrieving
// the hashed password from the cache.
type CachingStorage interface {
	UserEntryWithCacheHash(conn *Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, CacheState, error)
}

// NewMysqlNativeAuthMethod will create a new AuthMethod that implements the
// `mysql_native_password` handshake. The caller will need to provide a storage
// object and validator that will be called during the handshake phase.
func NewMysqlNativeAuthMethod(layer HashStorage, validator UserValidator) AuthMethod {
	authMethod := mysqlNativePasswordAuthMethod{
		storage:   layer,
		validator: validator,
	}
	return &authMethod
}

// NewMysqlClearAuthMethod will create a new AuthMethod that implements the
// `mysql_clear_password` handshake. The caller will need to provide a storage
// object for plain text passwords and validator that will be called during the
// handshake phase.
func NewMysqlClearAuthMethod(layer PlainTextStorage, validator UserValidator) AuthMethod {
	authMethod := mysqlClearAuthMethod{
		storage:   layer,
		validator: validator,
	}
	return &authMethod
}

// Constants for the dialog plugin.
const (
	// Default message if no custom message
	// is configured. This is used when the message
	// is the empty string.
	mysqlDialogDefaultMessage = "Enter password: "

	// Dialog plugin is similar to clear text, but can respond to multiple
	// prompts in a row. This is not yet implemented.
	// Follow questions should be prepended with a `cmd` byte:
	// 0x02 - ordinary question
	// 0x03 - last question
	// 0x04 - password question
	// 0x05 - last password
	mysqlDialogAskPassword = 0x04
)

// NewMysqlDialogAuthMethod will create a new AuthMethod that implements the
// `dialog` handshake. The caller will need to provide a storage object for plain
// text passwords and validator that will be called during the handshake phase.
// The message given will be sent as part of the dialog. If the empty string is
// provided, the default message of "Enter password: " will be used.
func NewMysqlDialogAuthMethod(layer PlainTextStorage, validator UserValidator, msg string) AuthMethod {
	if msg == "" {
		msg = mysqlDialogDefaultMessage
	}

	authMethod := mysqlDialogAuthMethod{
		storage:   layer,
		validator: validator,
		msg:       msg,
	}
	return &authMethod
}

// NewSha2CachingAuthMethod will create a new AuthMethod that implements the
// `caching_sha2_password` handshake. The caller will need to provide a cache
// object for the fast auth path and a plain text storage object that will
// be called if the return of the first layer indicates the full auth dance is
// needed.
//
// Right now we only support caching_sha2_password over TLS or a Unix socket.
//
// If TLS is not enabled, the client needs to encrypt it with the public
// key of the server. In that case, Vitess is already configured with
// a certificate anyway, so we recommend to use TLS if you want to use
// caching_sha2_password in that case instead of allowing the plain
// text fallback path here.
//
// This might change in the future if there's a good argument and implementation
// for allowing the plain text path here as well.
func NewSha2CachingAuthMethod(layer1 CachingStorage, layer2 FullAuthStorage, validator UserValidator) AuthMethod {
	authMethod := mysqlCachingSha2AuthMethod{
		cache:     layer1,
		storage:   layer2,
		validator: validator,
	}
	return &authMethod
}

// ScrambleMysqlNativePassword computes the hash of the password using 4.1+ method.
//
// This can be used for example inside a `mysql_native_password` plugin implementation
// if the backend storage implements storage of plain text passwords.
func ScrambleMysqlNativePassword(salt, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(salt + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)
	// outer Hash
	crypt.Reset()
	crypt.Write(salt)
	crypt.Write(hash)
	scramble := crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// DecodeMysqlNativePasswordHex decodes the standard format used by MySQL
// for 4.1 style password hashes. It drops the optionally leading * before
// decoding the rest as a hex encoded string.
func DecodeMysqlNativePasswordHex(hexEncodedPassword string) ([]byte, error) {
	if hexEncodedPassword[0] == '*' {
		hexEncodedPassword = hexEncodedPassword[1:]
	}
	return hex.DecodeString(hexEncodedPassword)
}

// VerifyHashedMysqlNativePassword verifies a client reply against a stored hash.
//
// This can be used for example inside a `mysql_native_password` plugin implementation
// if the backend storage where the stored password is a SHA1(SHA1(password)).
//
// All values here are non encoded byte slices, so if you store for example the double
// SHA1 of the password as hex encoded characters, you need to decode that first.
// See DecodeMysqlNativePasswordHex for a decoding helper for the standard encoding
// format of this hash used by MySQL.
func VerifyHashedMysqlNativePassword(reply, salt, hashedNativePassword []byte) bool {
	if len(reply) == 0 || len(hashedNativePassword) == 0 {
		return false
	}

	// scramble = SHA1(salt+hash)
	crypt := sha1.New()
	crypt.Write(salt)
	crypt.Write(hashedNativePassword)
	scramble := crypt.Sum(nil)

	for i := range scramble {
		scramble[i] ^= reply[i]
	}
	hashStage1 := scramble

	crypt.Reset()
	crypt.Write(hashStage1)
	candidateHash2 := crypt.Sum(nil)

	return subtle.ConstantTimeCompare(candidateHash2, hashedNativePassword) == 1
}

// VerifyHashedCachingSha2Password verifies a client reply against a stored hash.
//
// This can be used for example inside a `caching_sha2_password` plugin implementation
// if the cache storage uses password keys with SHA256(SHA256(password)).
//
// All values here are non encoded byte slices, so if you store for example the double
// SHA256 of the password as hex encoded characters, you need to decode that first.
func VerifyHashedCachingSha2Password(reply, salt, hashedCachingSha2Password []byte) bool {
	if len(reply) == 0 || len(hashedCachingSha2Password) == 0 {
		return false
	}

	crypt := sha256.New()
	crypt.Write(hashedCachingSha2Password)
	crypt.Write(salt)
	scramble := crypt.Sum(nil)

	// token = scramble XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= reply[i]
	}
	hashStage1 := scramble

	crypt.Reset()
	crypt.Write(hashStage1)
	candidateHash2 := crypt.Sum(nil)

	return subtle.ConstantTimeCompare(candidateHash2, hashedCachingSha2Password) == 1
}

func b64From24bit(b []byte, n int, buf *bytes.Buffer) {
	b64t := []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	w := (int64(b[0]) << 16) | (int64(b[1]) << 8) | int64(b[2])
	for n > 0 {
		n--
		buf.WriteByte(b64t[w&0x3f])
		w >>= 6
	}
}

// Sha256Hash is an util function to calculate sha256 hash.
func Sha256Hash(input []byte) []byte {
	res := sha256.Sum256(input)
	return res[:]
}

// ScrambleSha2Password computes the hash of the password using SHA256 as required by
// caching_sha2 password plugin for full authentication
func ScrambleSha2Password(plaintext string, pwhash []byte) (string, error) {
	pwhashParts := bytes.Split(pwhash, []byte("$"))
	if len(pwhashParts) != 4 {
		return "", errors.New("failed to decode hash parts")
	}
	hashType := string(pwhashParts[1])
	if hashType != "A" {
		return "", errors.New("digest type is incompatible")
	}
	iterations, err := strconv.ParseInt(string(pwhashParts[2]), 16, 64)
	if err != nil {
		return "", err
	}
	iterations = iterations * ITERATERMULNUM
	salt := pwhashParts[3][:SALTLENGTH]

	// 1, 2, 3
	bufA := bytes.NewBuffer(make([]byte, 0, 4096))
	bufA.WriteString(plaintext)
	bufA.Write(salt)

	// 4, 5, 6, 7, 8
	bufB := bytes.NewBuffer(make([]byte, 0, 4096))
	bufB.WriteString(plaintext)
	bufB.Write(salt)
	bufB.WriteString(plaintext)
	sumB := Sha256Hash(bufB.Bytes())
	bufB.Reset()

	// 9, 10
	var i int
	for i = len(plaintext); i > MIXCHARS; i -= MIXCHARS {
		bufA.Write(sumB[:MIXCHARS])
	}
	bufA.Write(sumB[:i])
	// 11
	for i = len(plaintext); i > 0; i >>= 1 {
		if i%2 == 0 {
			bufA.WriteString(plaintext)
		} else {
			bufA.Write(sumB[:])
		}
	}

	// 12
	sumA := Sha256Hash(bufA.Bytes())
	bufA.Reset()

	// 13, 14, 15
	bufDP := bufA
	for range []byte(plaintext) {
		bufDP.WriteString(plaintext)
	}
	sumDP := Sha256Hash(bufDP.Bytes())
	bufDP.Reset()

	// 16
	p := make([]byte, 0, sha256.Size)
	for i = len(plaintext); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			p = append(p, sumDP[:]...)
		} else {
			p = append(p, sumDP[0:i]...)
		}
	}
	// 17, 18, 19
	bufDS := bufA
	for i = 0; i < 16+int(sumA[0]); i++ {
		bufDS.Write(salt)
	}
	sumDS := Sha256Hash(bufDS.Bytes())
	bufDS.Reset()

	// 20
	s := make([]byte, 0, 32)
	for i = len(salt); i > 0; i -= MIXCHARS {
		if i > MIXCHARS {
			s = append(s, sumDS[:]...)
		} else {
			s = append(s, sumDS[0:i]...)
		}
	}

	// 21
	bufC := bufA
	var sumC []byte
	for i = 0; i < int(iterations); i++ {
		bufC.Reset()
		if i&1 != 0 {
			bufC.Write(p)
		} else {
			bufC.Write(sumA[:])
		}
		if i%3 != 0 {
			bufC.Write(s)
		}
		if i%7 != 0 {
			bufC.Write(p)
		}
		if i&1 != 0 {
			bufC.Write(sumA[:])
		} else {
			bufC.Write(p)
		}
		sumC = Sha256Hash(bufC.Bytes())
		sumA = sumC
	}
	// 22
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	buf.Write([]byte{'$', 'A', '$'})
	rounds := fmt.Sprintf("%03X", iterations/ITERATERMULNUM)
	buf.WriteString(rounds)
	buf.Write([]byte{'$'})
	buf.Write(salt)

	b64From24bit([]byte{sumC[0], sumC[10], sumC[20]}, 4, buf)
	b64From24bit([]byte{sumC[21], sumC[1], sumC[11]}, 4, buf)
	b64From24bit([]byte{sumC[12], sumC[22], sumC[2]}, 4, buf)
	b64From24bit([]byte{sumC[3], sumC[13], sumC[23]}, 4, buf)
	b64From24bit([]byte{sumC[24], sumC[4], sumC[14]}, 4, buf)
	b64From24bit([]byte{sumC[15], sumC[25], sumC[5]}, 4, buf)
	b64From24bit([]byte{sumC[6], sumC[16], sumC[26]}, 4, buf)
	b64From24bit([]byte{sumC[27], sumC[7], sumC[17]}, 4, buf)
	b64From24bit([]byte{sumC[18], sumC[28], sumC[8]}, 4, buf)
	b64From24bit([]byte{sumC[9], sumC[19], sumC[29]}, 4, buf)
	b64From24bit([]byte{0, sumC[31], sumC[30]}, 3, buf)

	return buf.String(), nil
}

// ScramblePassword return SHA256(SHA256(password))
func ScramblePassword(password []byte) []byte {
	// Compute SHA256(password)
	hash := sha256.New()
	hash.Write(password)
	passwordHash := hash.Sum(nil)

	return passwordHash
}

// XOR(password, SHA256(password, salt))
func XORHashAndSalt(password []byte, salt []byte) []byte {
	// Compute SHA256(password)
	hash := sha256.New()
	hash.Write(password)
	passwordHash := hash.Sum(nil)

	// Compute SHA256(password + salt)
	hash.Reset()
	hash.Write(passwordHash)
	hash.Write(salt)
	passwordSaltHash := hash.Sum(nil)

	// XOR two hashes
	for i := range passwordSaltHash {
		password[i] ^= passwordSaltHash[i]
	}

	return password
}

// ScrambleCachingSha2Password computes the hash of the password using SHA256 as required by
// caching_sha2_password plugin for "fast" authentication
// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), salt))
func ScrambleCachingSha2Password(salt []byte, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA256(password)
	crypt := sha256.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA256(SHA256(stage1Hash) + salt)
	crypt.Reset()
	crypt.Write(stage1)
	innerHash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(innerHash)
	crypt.Write(salt)
	scramble := crypt.Sum(nil)

	// token = stage1Hash XOR scrambleHash
	for i := range stage1 {
		stage1[i] ^= scramble[i]
	}

	return stage1
}

// EncryptPasswordWithPublicKey obfuscates the password and encrypts it with server's public key as required by
// caching_sha2_password plugin for "full" authentication
func EncryptPasswordWithPublicKey(salt []byte, password []byte, pub *rsa.PublicKey) ([]byte, error) {
	if len(password) == 0 {
		return nil, nil
	}

	buffer := make([]byte, len(password)+1)
	copy(buffer, password)
	for i := range buffer {
		buffer[i] ^= salt[i%len(salt)]
	}

	sha1Hash := sha1.New()
	enc, err := rsa.EncryptOAEP(sha1Hash, rand.Reader, pub, buffer, nil)
	if err != nil {
		return nil, err
	}

	return enc, nil
}

type mysqlNativePasswordAuthMethod struct {
	storage   HashStorage
	validator UserValidator
}

func (n *mysqlNativePasswordAuthMethod) Name() AuthMethodDescription {
	return MysqlNativePassword
}

func (n *mysqlNativePasswordAuthMethod) HandleUser(conn *Conn, user string) bool {
	return n.validator.HandleUser(user, "mysql_native_password")
}

func (n *mysqlNativePasswordAuthMethod) AuthPluginData() ([]byte, error) {
	salt, err := newSalt()
	if err != nil {
		return nil, err
	}
	return append(salt, 0), nil
}

func (n *mysqlNativePasswordAuthMethod) AllowClearTextWithoutTLS() bool {
	return true
}

func (n *mysqlNativePasswordAuthMethod) HandleAuthPluginData(conn *Conn, user string, serverAuthPluginData []byte, clientAuthPluginData []byte, remoteAddr net.Addr) (Getter, error) {
	if serverAuthPluginData[len(serverAuthPluginData)-1] != 0x00 {
		return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	salt := serverAuthPluginData[:len(serverAuthPluginData)-1]
	return n.storage.UserEntryWithHash(conn, salt, user, clientAuthPluginData, remoteAddr)
}

type mysqlClearAuthMethod struct {
	storage   PlainTextStorage
	validator UserValidator
}

func (n *mysqlClearAuthMethod) Name() AuthMethodDescription {
	return MysqlClearPassword
}

func (n *mysqlClearAuthMethod) HandleUser(conn *Conn, user string) bool {
	return n.validator.HandleUser(user, "mysql_clear_password")
}

func (n *mysqlClearAuthMethod) AuthPluginData() ([]byte, error) {
	return nil, nil
}

func (n *mysqlClearAuthMethod) AllowClearTextWithoutTLS() bool {
	return false
}

func (n *mysqlClearAuthMethod) HandleAuthPluginData(conn *Conn, user string, serverAuthPluginData []byte, clientAuthPluginData []byte, remoteAddr net.Addr) (Getter, error) {
	return n.storage.UserEntryWithPassword(conn, user, string(clientAuthPluginData[:len(clientAuthPluginData)-1]), remoteAddr)
}

type mysqlDialogAuthMethod struct {
	storage   PlainTextStorage
	validator UserValidator
	msg       string
}

func (n *mysqlDialogAuthMethod) Name() AuthMethodDescription {
	return MysqlDialog
}

func (n *mysqlDialogAuthMethod) HandleUser(conn *Conn, user string) bool {
	return n.validator.HandleUser(user, "mysql_dialog_password")
}

func (n *mysqlDialogAuthMethod) AuthPluginData() ([]byte, error) {
	result := make([]byte, len(n.msg)+2)
	result[0] = mysqlDialogAskPassword
	writeNullString(result, 1, n.msg)
	return result, nil
}

func (n *mysqlDialogAuthMethod) HandleAuthPluginData(conn *Conn, user string, serverAuthPluginData []byte, clientAuthPluginData []byte, remoteAddr net.Addr) (Getter, error) {
	return n.storage.UserEntryWithPassword(conn, user, string(clientAuthPluginData[:len(clientAuthPluginData)-1]), remoteAddr)
}

func (n *mysqlDialogAuthMethod) AllowClearTextWithoutTLS() bool {
	return false
}

type mysqlCachingSha2AuthMethod struct {
	cache     CachingStorage
	storage   FullAuthStorage
	validator UserValidator
}

func (n *mysqlCachingSha2AuthMethod) Name() AuthMethodDescription {
	return CachingSha2Password
}

func (n *mysqlCachingSha2AuthMethod) HandleUser(conn *Conn, user string) bool {
	if !conn.TLSEnabled() && !conn.IsUnixSocket() {
		return false
	}
	return n.validator.HandleUser(user, "caching_sha2_password")
}

func (n *mysqlCachingSha2AuthMethod) AuthPluginData() ([]byte, error) {
	salt, err := newSalt()
	if err != nil {
		return nil, err
	}
	return append(salt, 0), nil
}

func (n *mysqlCachingSha2AuthMethod) AllowClearTextWithoutTLS() bool {
	return true
}

func (n *mysqlCachingSha2AuthMethod) HandleAuthPluginData(c *Conn, user string, serverAuthPluginData []byte, clientAuthPluginData []byte, remoteAddr net.Addr) (Getter, error) {
	if serverAuthPluginData[len(serverAuthPluginData)-1] != 0x00 {
		return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}

	salt := serverAuthPluginData[:len(serverAuthPluginData)-1]
	result, cacheState, err := n.cache.UserEntryWithCacheHash(c, salt, user, clientAuthPluginData, remoteAddr)

	if err != nil {
		return nil, err
	}

	switch cacheState {
	case AuthRejected:
		return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	case AuthAccepted:
		// We need to write a more data packet to indicate the
		// handshake completed properly. This  will be followed
		// by a regular OK packet which the caller of this method will send.
		data, pos := c.startEphemeralPacketWithHeader(2)
		pos = writeByte(data, pos, AuthMoreDataPacket)
		_ = writeByte(data, pos, CachingSha2FastAuth)
		err = c.writeEphemeralPacket()
		if err != nil {
			return nil, err
		}
		return result, nil
	case AuthNeedMoreData:
		if !c.TLSEnabled() && !c.IsUnixSocket() {
			return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
		}

		data, pos := c.startEphemeralPacketWithHeader(2)
		pos = writeByte(data, pos, AuthMoreDataPacket)
		writeByte(data, pos, CachingSha2FullAuth)
		c.writeEphemeralPacket()

		password, err := readPacketPasswordString(c)
		if err != nil {
			return nil, err
		}

		return n.storage.UserEntryWithFullAuth(c, salt, user, password, remoteAddr)
	default:
		// Somehow someone returned an unknown state, let's error with access denied.
		return nil, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
}

// authServers is a registry of AuthServer implementations.
var authServers = make(map[string]AuthServer)

// mu is used to lock access to authServers
var mu sync.Mutex

// RegisterAuthServer registers an implementations of AuthServer.
func RegisterAuthServer(name string, authServer AuthServer) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := authServers[name]; ok {
		log.Fatalf("AuthServer named %v already exists", name)
	}
	authServers[name] = authServer
}

// GetAuthServer returns an AuthServer by name, or log.Exitf.
func GetAuthServer(name string) AuthServer {
	mu.Lock()
	defer mu.Unlock()
	authServer, ok := authServers[name]
	if !ok {
		log.Exitf("no AuthServer name %v registered", name)
	}
	return authServer
}

func newSalt() ([]byte, error) {
	salt := make([]byte, 20)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	// Salt must be a legal UTF8 string.
	for i := 0; i < len(salt); i++ {
		salt[i] &= 0x7f
		if salt[i] == '\x00' || salt[i] == '$' {
			salt[i]++
		}
	}

	return salt, nil
}

func negotiateAuthMethod(conn *Conn, as AuthServer, user string, requestedAuth AuthMethodDescription) (AuthMethod, error) {
	for _, m := range as.AuthMethods() {
		if m.Name() == requestedAuth && m.HandleUser(conn, user) {
			return m, nil
		}
	}
	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown auth method requested: %s", string(requestedAuth))
}

func readPacketPasswordString(c *Conn) (string, error) {
	// Read a packet, the password is the payload, as a
	// zero terminated string.
	data, err := c.ReadPacket()
	if err != nil {
		return "", err
	}
	if len(data) == 0 || data[len(data)-1] != 0 {
		return "", vterrors.Errorf(vtrpc.Code_INTERNAL, "received invalid response packet, datalen=%v", len(data))
	}
	return string(data[:len(data)-1]), nil
}
