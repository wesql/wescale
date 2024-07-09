/*
Copyright 2021 The Vitess Authors.

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

package collations

import (
	"math"

	"vitess.io/vitess/go/mysql/collations/internal/charset"
)

// CaseAwareCollation implements lowercase and uppercase conventions for collations.
type CaseAwareCollation interface {
	Collation
	ToUpper(dst []byte, src []byte) []byte
	ToLower(dst []byte, src []byte) []byte
}

// ID is a numeric identifier for a collation. These identifiers are defined by MySQL, not by Vitess.
type ID uint16

// Unknown is the default ID for an unknown collation.
const Unknown ID = 0

// Collation implements a MySQL-compatible collation. It defines how to compare
// for sorting order and equality two strings with the same encoding.
type Collation interface {
	// Init initializes the internal state for the collation the first time it is used
	Init()

	// ID returns the numerical identifier for this collation. This is the same
	// value that is returned by MySQL in a query's headers to identify the collation
	// for a given column
	ID() ID

	// Name is the full name of this collation, in the form of "ENCODING_LANG_SENSITIVITY"
	Name() string

	// Collate compares two strings using this collation. `left` and `right` must be the
	// two strings encoded in the proper encoding for this collation. If `isPrefix` is true,
	// the function instead behaves equivalently to `strings.HasPrefix(left, right)`, but
	// being collation-aware.
	// It returns a numeric value like a normal comparison function: <0 if left < right,
	// 0 if left == right, >0 if left > right
	Collate(left, right []byte, isPrefix bool) int

	// WeightString returns a weight string for the given `src` string. A weight string
	// is a binary representation of the weights for the given string, that can be
	// compared byte-wise to return identical results to collating this string.
	//
	// This means:
	//		bytes.Compare(WeightString(left), WeightString(right)) == Collate(left, right)
	//
	// The semantics of this API have been carefully designed to match MySQL's behavior
	// in its `strnxfrm` API. Most notably, the `numCodepoints` argument implies different
	// behaviors depending on the collation's padding mode:
	//
	// - For collations that pad WITH SPACE (this is, all legacy collations in MySQL except
	//	for the newly introduced UCA v9.0.0 utf8mb4 collations in MySQL 8.0), `numCodepoints`
	// 	can have the following values:
	//
	//		- if `numCodepoints` is any integer greater than zero, this treats the `src` string
	//		as if it were in a `CHAR(numCodepoints)` column in MySQL, meaning that the resulting
	//		weight string will be padded with the weight for the SPACE character until it becomes
	//		wide enough to fill the `CHAR` column. This is necessary to perform weight comparisons
	//		in fixed-`CHAR` columns. If `numCodepoints` is smaller than the actual amount of
	//		codepoints stored in `src`, the result is unspecified.
	//
	//		- if `numCodepoints` is zero, this is equivalent to `numCodepoints = RuneCount(src)`,
	//		meaning that the resulting weight string will have no padding at the end: it'll only have
	//		the weight values for the exact amount of codepoints contained in `src`. This is the
	//		behavior required to sort `VARCHAR` columns.
	//
	//		- if `numCodepoints` is the special constant PadToMax, then the `dst` slice must be
	//		pre-allocated to a zero-length slice with enough capacity to hold the complete weight
	//		string, and any remaining capacity in `dst` will be filled by the weights for the
	//		padding character, repeatedly. This is a special flag used by MySQL when performing
	//		filesorts, where all the sorting keys must have identical sizes, even for `VARCHAR`
	//		columns.
	//
	//	- For collations that have NO PAD (this is, the newly introduced UCA v9.0.0 utf8mb4 collations
	//	in MySQL 8.0), `numCodepoints` can only have the special constant `PadToMax`, which will make
	//	the weight string padding equivalent to a PAD SPACE collation (as explained in the previous
	//	section). All other values for `numCodepoints` are ignored, because NO PAD collations always
	//	return the weights for the codepoints in their strings, with no further padding at the end.
	//
	// The resulting weight string is written to `dst`, which can be pre-allocated to
	// WeightStringLen() bytes to prevent growing the slice. `dst` can also be nil, in which
	// case it will grow dynamically. If `numCodepoints` has the special PadToMax value explained
	// earlier, `dst` MUST be pre-allocated to the target size or the function will return an
	// empty slice.
	WeightString(dst, src []byte, numCodepoints int) []byte

	// WeightStringLen returns a size (in bytes) that would fit any weight strings for a string
	// with `numCodepoints` using this collation. Note that this is a higher bound for the size
	// of the string, and in practice weight strings can be significantly smaller than the
	// returned value.
	WeightStringLen(numCodepoints int) int

	// Hash returns a 32 or 64 bit identifier (depending on the platform) that uniquely identifies
	// the given string based on this collation. It is functionally equivalent to calling WeightString
	// and then hashing the result.
	//
	// Consequently, if the hashes for two strings are different, then the two strings are considered
	// different according to this collation. If the hashes for two strings are equal, the two strings
	// may or may not be considered equal according to this collation, because hashes can collide unlike
	// weight strings.
	//
	// The numCodepoints argument has the same behavior as in WeightString: if this collation uses PAD SPACE,
	// the hash will interpret the source string as if it were stored in a `CHAR(n)` column. If the value of
	// numCodepoints is 0, this is equivalent to setting `numCodepoints = RuneCount(src)`.
	// For collations with NO PAD, the numCodepoint argument is ignored.
	Hash(src []byte, numCodepoints int) HashCode

	// Wildcard returns a matcher for the given wildcard pattern. The matcher can be used to repeatedly
	// test different strings to check if they match the pattern. The pattern must be a traditional wildcard
	// pattern, which may contain the provided special characters for matching one character or several characters.
	// The provided `escape` character will be used as an escape sequence in front of the other special characters.
	//
	// This method is fully collation aware; the matching will be performed according to the underlying collation.
	// I.e. if this is a case-insensitive collation, matching will be case-insensitive.
	//
	// The returned WildcardPattern is always valid, but if the provided special characters do not exist in this
	// collation's repertoire, the returned pattern will not match any strings. Likewise, if the provided pattern
	// has invalid syntax, the returned pattern will not match any strings.
	//
	// If the provided special characters are 0, the defaults to parse an SQL 'LIKE' statement will be used.
	// This is, '_' for matching one character, '%' for matching many and '\\' for escape.
	//
	// This method can also be used for Shell-like matching with '?', '*' and '\\' as their respective special
	// characters.
	Wildcard(pat []byte, matchOne, matchMany, escape rune) WildcardPattern

	// Charset returns the Charset with which this collation is encoded
	Charset() charset.Charset

	// IsBinary returns whether this collation is a binary collation
	IsBinary() bool
}

type HashCode = uintptr

// WildcardPattern is a matcher for a wildcard pattern, constructed from a given collation
type WildcardPattern interface {
	// Match returns whether the given string matches this pattern
	Match(in []byte) bool
}

const PadToMax = math.MaxInt32

func minInt(i1, i2 int) int {
	if i1 < i2 {
		return i1
	}
	return i2
}

var globalAllCollations = make(map[ID]Collation)

var CollationNameToID = map[string]ID{"utf16_czech_ci": 111, "utf32_latvian_ci": 162, "utf8mb4_czech_ci": 234, "hebrew_bin": 71, "koi8u_bin": 75, "sjis_bin": 88, "utf8_polish_ci": 197, "utf8mb4_lv_0900_ai_ci": 258, "utf16le_general_ci": 56, "utf8_persian_ci": 208, "utf32_general_ci": 60, "cp850_bin": 80, "ucs2_persian_ci": 144, "utf8_turkish_ci": 201, "utf8_hungarian_ci": 210, "utf8mb4_persian_ci": 240, "utf8mb4_latvian_ci": 226, "utf8mb4_unicode_520_ci": 246, "koi8r_general_ci": 7, "cp1256_general_ci": 57, "ascii_bin": 65, "gb2312_bin": 86, "utf32_polish_ci": 165, "utf8_unicode_520_ci": 214, "utf32_turkish_ci": 169, "utf8mb4_slovak_ci": 237, "utf8mb4_esperanto_ci": 241, "macroman_bin": 53, "utf16_sinhala_ci": 120, "utf32_estonian_ci": 166, "latin2_croatian_ci": 27, "cp866_general_ci": 36, "ucs2_romanian_ci": 131, "utf32_czech_ci": 170, "utf32_danish_ci": 171, "utf8mb4_da_0900_ai_ci": 267, "utf16_hungarian_ci": 119, "utf8mb4_romanian_ci": 227, "utf8mb4_0900_ai_ci": 255, "latin7_general_cs": 42, "utf16_lithuanian_ci": 113, "utf8_german2_ci": 212, "utf8mb4_slovenian_ci": 228, "utf8mb4_es_trad_0900_ai_ci": 270, "utf8_general_ci": 33, "cp932_japanese_ci": 95, "ucs2_czech_ci": 138, "utf8mb4_turkish_ci": 233, "utf8mb4_cs_0900_ai_ci": 266, "utf8mb4_hr_0900_as_cs": 298, "hp8_english_ci": 6, "utf8mb4_sl_0900_ai_ci": 260, "utf8mb4_vi_0900_as_cs": 300, "latin1_german1_ci": 5, "cp866_bin": 68, "ucs2_bin": 90, "utf8_romanian_ci": 195, "latin7_general_ci": 41, "euckr_bin": 85, "ucs2_german2_ci": 148, "utf8_danish_ci": 203, "utf8mb4_danish_ci": 235, "utf8mb4_hu_0900_as_cs": 297, "ucs2_polish_ci": 133, "utf32_romanian_ci": 163, "utf8_latvian_ci": 194, "utf8mb4_de_pb_0900_as_cs": 279, "utf8mb4_la_0900_as_cs": 294, "latin1_bin": 47, "cp1257_bin": 58, "ucs2_unicode_520_ci": 150, "utf32_sinhala_ci": 179, "utf8_spanish_ci": 199, "utf8mb4_sk_0900_as_cs": 292, "greek_bin": 70, "cp1251_general_ci": 51, "utf8_icelandic_ci": 193, "utf8_spanish2_ci": 206, "macroman_general_ci": 39, "cp852_general_ci": 40, "utf8_bin": 83, "utf8mb4_hr_0900_ai_ci": 275, "utf8mb4_es_0900_as_cs": 286, "ucs2_vietnamese_ci": 151, "utf32_spanish_ci": 167, "utf8mb4_spanish2_ci": 238, "koi8u_general_ci": 22, "keybcs2_bin": 73, "koi8r_bin": 74, "ucs2_hungarian_ci": 146, "macce_bin": 43, "utf16_bin": 55, "utf8mb4_hungarian_ci": 242, "utf8mb4_hu_0900_ai_ci": 274, "utf16_esperanto_ci": 118, "utf8mb4_ja_0900_as_cs_ks": 304, "cp1251_bulgarian_ci": 14, "utf16_danish_ci": 112, "utf32_icelandic_ci": 161, "utf32_swedish_ci": 168, "swe7_swedish_ci": 10, "utf8mb4_sv_0900_ai_ci": 264, "utf8mb4_sl_0900_as_cs": 283, "utf16_general_ci": 54, "cp932_bin": 96, "utf16_german2_ci": 121, "ucs2_roman_ci": 143, "utf32_spanish2_ci": 174, "utf8_unicode_ci": 192, "ucs2_esperanto_ci": 145, "utf8mb4_croatian_ci": 245, "latin1_danish_ci": 15, "utf32_unicode_520_ci": 182, "utf8mb4_eo_0900_as_cs": 296, "latin7_estonian_cs": 20, "cp1251_ukrainian_ci": 23, "utf16_turkish_ci": 110, "ucs2_latvian_ci": 130, "utf32_hungarian_ci": 178, "utf8mb4_lv_0900_as_cs": 281, "ucs2_general_ci": 35, "utf16_croatian_ci": 122, "utf32_roman_ci": 175, "utf32_croatian_ci": 181, "utf8_slovenian_ci": 196, "utf8mb4_ru_0900_as_cs": 307, "euckr_korean_ci": 19, "utf8mb4_general_ci": 45, "ucs2_spanish_ci": 135, "utf32_persian_ci": 176, "utf8_roman_ci": 207, "armscii8_general_ci": 32, "utf16_latvian_ci": 103, "utf8_vietnamese_ci": 215, "latin5_turkish_ci": 30, "utf16_icelandic_ci": 102, "ucs2_croatian_ci": 149, "utf32_esperanto_ci": 177, "gb2312_chinese_ci": 24, "eucjpms_japanese_ci": 97, "utf16_spanish_ci": 108, "utf16_roman_ci": 116, "sjis_japanese_ci": 13, "utf16_romanian_ci": 104, "utf16_unicode_520_ci": 123, "ucs2_danish_ci": 139, "utf8mb4_de_pb_0900_ai_ci": 256, "utf8mb4_ro_0900_as_cs": 282, "latin1_swedish_ci": 8, "cp1257_lithuanian_ci": 29, "cp1250_bin": 66, "utf16_estonian_ci": 107, "utf8mb4_pl_0900_ai_ci": 261, "utf8mb4_tr_0900_as_cs": 288, "ucs2_swedish_ci": 136, "utf16le_bin": 62, "ucs2_estonian_ci": 134, "utf8mb4_roman_ci": 239, "utf8mb4_vi_0900_ai_ci": 277, "utf8mb4_cs_0900_as_cs": 289, "utf8mb4_zh_0900_as_cs": 308, "hebrew_general_ci": 16, "latin2_bin": 77, "utf8_czech_ci": 202, "utf8mb4_ja_0900_as_cs": 303, "dec8_bin": 69, "utf8_estonian_ci": 198, "utf8mb4_ro_0900_ai_ci": 259, "utf8mb4_es_trad_0900_as_cs": 293, "binary": 63, "ascii_general_ci": 11, "utf16_unicode_ci": 101, "utf32_lithuanian_ci": 172, "utf8mb4_es_0900_ai_ci": 263, "utf8mb4_la_0900_ai_ci": 271, "utf8mb4_0900_as_cs": 278, "dec8_swedish_ci": 3, "latin2_hungarian_ci": 21, "cp1250_croatian_ci": 44, "ucs2_icelandic_ci": 129, "utf32_vietnamese_ci": 183, "utf8mb4_icelandic_ci": 225, "utf16_persian_ci": 117, "ucs2_unicode_ci": 128, "utf8mb4_estonian_ci": 230, "utf8mb4_german2_ci": 244, "utf8mb4_pl_0900_as_cs": 284, "utf8mb4_da_0900_as_cs": 290, "keybcs2_general_ci": 37, "cp1251_bin": 50, "utf32_unicode_ci": 160, "cp1251_general_cs": 52, "latin5_bin": 78, "geostd8_general_ci": 92, "eucjpms_bin": 98, "ucs2_lithuanian_ci": 140, "ucs2_slovak_ci": 141, "utf16_slovenian_ci": 105, "utf8mb4_polish_ci": 229, "utf8mb4_eo_0900_ai_ci": 273, "utf16_slovak_ci": 114, "utf8mb4_sk_0900_ai_ci": 269, "utf8_swedish_ci": 200, "utf8mb4_lt_0900_ai_ci": 268, "latin1_general_cs": 49, "geostd8_bin": 93, "gb18030_unicode_520_ci": 250, "greek_general_ci": 25, "utf16_spanish2_ci": 115, "ucs2_sinhala_ci": 147, "utf32_slovak_ci": 173, "utf32_german2_ci": 180, "utf8mb4_lithuanian_ci": 236, "latin2_general_ci": 9, "cp1250_general_ci": 26, "macce_general_ci": 38, "cp1257_general_ci": 59, "utf16_vietnamese_ci": 124, "utf8_lithuanian_ci": 204, "hp8_bin": 72, "utf8mb4_et_0900_ai_ci": 262, "utf8mb4_tr_0900_ai_ci": 265, "utf8mb4_is_0900_ai_ci": 257, "utf8mb4_sv_0900_as_cs": 287, "cp1256_bin": 67, "latin7_bin": 79, "cp852_bin": 81, "utf8_slovak_ci": 205, "utf8_croatian_ci": 213, "utf8mb4_vietnamese_ci": 247, "utf8mb4_bin": 46, "ujis_bin": 91, "utf16_polish_ci": 106, "utf8_esperanto_ci": 209, "utf8mb4_lt_0900_as_cs": 291, "utf8mb4_swedish_ci": 232, "utf8mb4_is_0900_as_cs": 280, "latin1_general_ci": 48, "cp1250_polish_ci": 99, "utf16_swedish_ci": 109, "ucs2_turkish_ci": 137, "ucs2_spanish2_ci": 142, "utf8mb4_unicode_ci": 224, "utf8mb4_et_0900_as_cs": 285, "utf8mb4_0900_as_ci": 305, "ujis_japanese_ci": 12, "utf32_bin": 61, "armscii8_bin": 64, "ucs2_slovenian_ci": 132, "utf8_sinhala_ci": 211, "utf8mb4_spanish_ci": 231, "utf32_slovenian_ci": 164, "utf8mb4_sinhala_ci": 243, "cp850_general_ci": 4, "swe7_bin": 82, "latin1_spanish_ci": 94, "utf8mb4_ru_0900_ai_ci": 306}

func register(c Collation) {
	if _, found := globalAllCollations[c.ID()]; found {
		panic("duplicated collation registered")
	}
	globalAllCollations[c.ID()] = c
}

// Slice returns the substring in `input[from:to]`, where `from` and `to`
// are collation-aware character indices instead of bytes.
func Slice(collation Collation, input []byte, from, to int) []byte {
	return charset.Slice(collation.Charset(), input, from, to)
}

// Validate returns whether the given `input` is properly encoded with the
// character set for the given collation.
func Validate(collation Collation, input []byte) bool {
	return charset.Validate(collation.Charset(), input)
}

// Convert converts the bytes in `src`, which are encoded in `srcCollation`'s charset,
// into a byte slice encoded in `dstCollation`'s charset. The resulting byte slice is
// appended to `dst` and returned.
func Convert(dst []byte, dstCollation Collation, src []byte, srcCollation Collation) ([]byte, error) {
	return charset.Convert(dst, dstCollation.Charset(), src, srcCollation.Charset())
}

// Length returns the number of codepoints in the input based on the given collation
func Length(collation Collation, input []byte) int {
	return charset.Length(collation.Charset(), input)
}
