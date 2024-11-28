package framework

import (
	"bytes"
	"fmt"
	"text/template"
)

type FilterBuilder struct {
	Name        string
	Description string
	Priority    string
	Status      string

	// Pattern fields
	Plans                string
	FullyQualifiedTables string
	QueryRegex           string
	QueryTemplate        string
	RequestIPRegex       string
	UserRegex            string
	LeadingCommentRegex  string
	TrailingCommentRegex string
	BindVarConds         string

	// Execute fields
	Action     string
	ActionArgs string
}

func NewFilterBuilder(filterName string) *FilterBuilder {
	return &FilterBuilder{
		Name:     filterName,
		Priority: "1000",
		Status:   "ACTIVE",
	}
}

func NewWasmFilterBuilder(filterName string, wasmName string) *FilterBuilder {
	return &FilterBuilder{
		Name:       filterName,
		Priority:   "1000",
		Status:     "ACTIVE",
		Action:     "wasm_plugin",
		ActionArgs: fmt.Sprintf("wasm_binary_name=\"%v\"", wasmName),
	}
}

func (fb *FilterBuilder) SetName(name string) *FilterBuilder {
	fb.Name = name
	return fb
}

func (fb *FilterBuilder) SetDescription(desc string) *FilterBuilder {
	fb.Description = desc
	return fb
}

func (fb *FilterBuilder) SetPriority(priority string) *FilterBuilder {
	fb.Priority = priority
	return fb
}

func (fb *FilterBuilder) SetStatus(status string) *FilterBuilder {
	fb.Status = status
	return fb
}

func (fb *FilterBuilder) SetPlans(plans string) *FilterBuilder {
	fb.Plans = plans
	return fb
}

func (fb *FilterBuilder) SetFullyQualifiedTables(tables string) *FilterBuilder {
	fb.FullyQualifiedTables = tables
	return fb
}

func (fb *FilterBuilder) SetQueryRegex(regex string) *FilterBuilder {
	fb.QueryRegex = regex
	return fb
}

func (fb *FilterBuilder) SetQueryTemplate(template string) *FilterBuilder {
	fb.QueryTemplate = template
	return fb
}

func (fb *FilterBuilder) SetRequestIPRegex(regex string) *FilterBuilder {
	fb.RequestIPRegex = regex
	return fb
}

func (fb *FilterBuilder) SetUserRegex(regex string) *FilterBuilder {
	fb.UserRegex = regex
	return fb
}

func (fb *FilterBuilder) SetLeadingCommentRegex(regex string) *FilterBuilder {
	fb.LeadingCommentRegex = regex
	return fb
}

func (fb *FilterBuilder) SetTrailingCommentRegex(regex string) *FilterBuilder {
	fb.TrailingCommentRegex = regex
	return fb
}

func (fb *FilterBuilder) SetBindVarConds(conds string) *FilterBuilder {
	fb.BindVarConds = conds
	return fb
}

func (fb *FilterBuilder) SetAction(action string) *FilterBuilder {
	fb.Action = action
	return fb
}

func (fb *FilterBuilder) SetActionArgs(args string) *FilterBuilder {
	fb.ActionArgs = args
	return fb
}

func (fb *FilterBuilder) Build() (string, error) {
	const sqlTemplate = `create filter if not exists {{.Name}} (
        desc='{{.Description}}',
        priority='{{.Priority}}',
        status='{{.Status}}'
)
with_pattern(
        plans='{{.Plans}}',
        fully_qualified_table_names='{{.FullyQualifiedTables}}',
        query_regex='{{.QueryRegex}}',
        query_template='{{.QueryTemplate}}',
        request_ip_regex='{{.RequestIPRegex}}',
        user_regex='{{.UserRegex}}',
        leading_comment_regex='{{.LeadingCommentRegex}}',
        trailing_comment_regex='{{.TrailingCommentRegex}}',
        bind_var_conds='{{.BindVarConds}}'
)
execute(
        action='{{.Action}}',
        action_args='{{.ActionArgs}}'
);`

	tmpl, err := template.New("sql").Parse(sqlTemplate)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, fb)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}
