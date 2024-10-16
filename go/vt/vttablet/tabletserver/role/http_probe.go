package role

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	mysqlRoleProbeUrlTemplate       = "http://%s:%d/v1.0/getrole"
	mysqlProbeServicePort     int64 = 3501
	mysqlProbeServiceHost           = "localhost"
)

const LORRY_HTTP_PORT_ENV_NAME = "LORRY_HTTP_PORT"
const LORRY_HTTP_HOST_ENV_NAME = "LORRY_HTTP_HOST"

func init() {
	servenv.OnParseFor("vttablet", registerHttpProbeFlags)
}

func registerHttpProbeFlags(fs *pflag.FlagSet) {
	fs.StringVar(&mysqlRoleProbeUrlTemplate, "mysql_role_probe_url_template", mysqlRoleProbeUrlTemplate, "mysql role probe url template")
}

func SetMysqlRoleProbeUrlTemplate(urlTemplate string) {
	mysqlRoleProbeUrlTemplate = urlTemplate
}

func httpProbe(ctx context.Context) (string, error) {
	setUpMysqlProbeServicePort()
	setUpMysqlProbeServiceHost()
	// curl -X GET -H 'Content-Type: application/json' 'http://localhost:3501/v1.0/getrole'
	getRoleUrl := fmt.Sprintf(mysqlRoleProbeUrlTemplate, mysqlProbeServiceHost, mysqlProbeServicePort)

	kvResp, err := doHttpProbe(ctx, http.MethodGet, getRoleUrl, nil)
	if err != nil {
		return UNKNOWN, fmt.Errorf("try to probe mysql role, but error happened: %v", err)
	}
	role, ok := kvResp["role"]
	if !ok {
		return UNKNOWN, fmt.Errorf("unable to get mysql role from probe response, response content: %v", kvResp)
	}

	// Safely assert the type of role to string.
	roleStr, ok := role.(string)
	if !ok {
		return UNKNOWN, fmt.Errorf("role value is not a string, role:%v", role)
	}
	return roleStr, nil
}

func doHttpProbe(ctx context.Context, method string, url string, params map[string]any) (map[string]any, error) {
	var reader io.Reader
	if params != nil {
		body, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("error marshalling params: %w", err)
		}
		reader = bytes.NewReader(body)
	}

	httpRequest, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, fmt.Errorf("error creating request for url %v: %w", url, err)
	}

	resp, err := http.DefaultClient.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusUnavailableForLegalReasons:
		return parseBody(resp.Body)
	case http.StatusNoContent:
		return nil, fmt.Errorf("status no content")
	default:
		msg, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %w", err)
		}
		return nil, fmt.Errorf("server responded with status code %d: %s", resp.StatusCode, msg)
	}
}

func parseBody(body io.Reader) (map[string]any, error) {
	result := map[string]any{}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed: %w", err)
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		log.Errorf("error unmarshalling response body, data: %v\n", data)
		return nil, fmt.Errorf("decode body failed: %w", err)
	}
	return result, nil
}

func setUpMysqlProbeServicePort() {
	portStr, ok := os.LookupEnv(LORRY_HTTP_PORT_ENV_NAME)
	if !ok {
		return
	}
	// parse portStr to int
	portFromEnv, err := strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		return
	}
	mysqlProbeServicePort = portFromEnv
}

func setUpMysqlProbeServiceHost() {
	host, ok := os.LookupEnv(LORRY_HTTP_HOST_ENV_NAME)
	if !ok {
		return
	}
	mysqlProbeServiceHost = host
}
