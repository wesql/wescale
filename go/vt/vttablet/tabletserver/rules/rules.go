/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

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

package rules

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

//-----------------------------------------------

const (
	DefaultPriority = 1000
	MinPriority     = 0
	DefaultStatus   = Active
)

const (
	bufferedTableRuleName = "buffered_table"
)

// Rules is used to store and execute rules for the tabletserver.
type Rules struct {
	rules []*Rule
}

func (qrs *Rules) ForEachRule(f func(rule *Rule)) {
	for _, rule := range qrs.rules {
		f(rule)
	}
}

// New creates a new Rules.
func New() *Rules {
	return &Rules{}
}

// Equal returns true if other is equal to this object, otherwise false.
func (qrs *Rules) Equal(other *Rules) bool {
	if len(qrs.rules) != len(other.rules) {
		return false
	}
	for i := 0; i < len(qrs.rules); i++ {
		if !qrs.rules[i].Equal(other.rules[i]) {
			return false
		}
	}
	return true
}

// Copy performs a deep copy of Rules.
// A nil input produces a nil output.
func (qrs *Rules) Copy() (newqrs *Rules) {
	newqrs = New()
	if qrs.rules != nil {
		newqrs.rules = make([]*Rule, 0, len(qrs.rules))
		for _, qr := range qrs.rules {
			newqrs.rules = append(newqrs.rules, qr.Copy())
		}
	}
	return newqrs
}

// CopyUnderlying makes a copy of the underlying rule array and returns it to
// the caller.
func (qrs *Rules) CopyUnderlying() []*Rule {
	cpy := make([]*Rule, 0, len(qrs.rules))
	for _, r := range qrs.rules {
		cpy = append(cpy, r.Copy())
	}
	return cpy
}

// Append merges the rules from another Rules into the receiver
func (qrs *Rules) Append(otherqrs *Rules) {
	qrs.rules = append(qrs.rules, otherqrs.rules...)
}

// Add adds a Rule to Rules. It does not check
// for duplicates.
func (qrs *Rules) Add(qr *Rule) {
	qrs.rules = append(qrs.rules, qr)
}

// Find finds the first occurrence of a Rule by matching
// the Name field. It returns nil if the rule was not found.
func (qrs *Rules) Find(name string) (qr *Rule) {
	for _, qr = range qrs.rules {
		if qr.Name == name {
			return qr
		}
	}
	return nil
}

// Delete deletes a Rule by name and returns the rule
// that was deleted. It returns nil if the rule was not found.
func (qrs *Rules) Delete(name string) (qr *Rule) {
	for i, qr := range qrs.rules {
		if qr.Name == name {
			for j := i; j < len(qrs.rules)-i-1; j++ {
				qrs.rules[j] = qrs.rules[j+1]
			}
			qrs.rules = qrs.rules[:len(qrs.rules)-1]
			return qr
		}
	}
	return nil
}

// UnmarshalJSON unmarshals Rules.
func (qrs *Rules) UnmarshalJSON(data []byte) (err error) {
	var rulesInfo []map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	err = dec.Decode(&rulesInfo)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}
	for _, ruleInfo := range rulesInfo {
		qr, err := BuildQueryRule(ruleInfo)
		if err != nil {
			return err
		}
		qrs.Add(qr)
	}
	return nil
}

// MarshalJSON marshals to JSON.
func (qrs *Rules) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	_, _ = b.WriteString("[")
	for i, rule := range qrs.rules {
		if i != 0 {
			_, _ = b.WriteString(",")
		}
		safeEncode(b, "", rule)
	}
	_, _ = b.WriteString("]")
	return b.Bytes(), nil
}

// FilterByPlan creates a new Rules by prefiltering on the query and planId. This allows
// us to create query plan specific Rules out of the original Rules. In the new rules,
// query, plans and fullyQualifiedTableNames predicates are empty.
func (qrs *Rules) FilterByPlan(query string, planid planbuilder.PlanType, tableNames ...string) (newqrs *Rules) {
	var newrules []*Rule
	for _, qr := range qrs.rules {
		if newrule := qr.FilterByPlan(query, planid, tableNames); newrule != nil {
			newrules = append(newrules, newrule)
		}
	}
	return &Rules{newrules}
}

// GetAction runs the input against the rules engine and returns the action to be performed.
func (qrs *Rules) GetAction(
	ip,
	user string,
	bindVars map[string]*querypb.BindVariable,
	marginComments sqlparser.MarginComments,
) (action Action, cancelCtx context.Context, desc string) {
	for _, qr := range qrs.rules {
		if act := qr.GetAction(ip, user, bindVars, marginComments); act != QRContinue {
			return act, qr.cancelCtx, qr.Description
		}
	}
	return QRContinue, nil, ""
}

//-----------------------------------------------

// Rule represents one rule (conditions-action).
// Name is meant to uniquely identify a rule.
// Description is a human readable comment that describes the rule.
// For a Rule to fire, all conditions of the Rule
// have to match. For example, an empty Rule will match
// all requests.
// Every Rule has an associated Action. If all the conditions
// of the Rule are met, then the Action is triggerred.
type Rule struct {
	Description string
	Name        string
	Priority    int
	Status      string

	// a rule can be dynamically cancelled. This function determines whether it is cancelled
	cancelCtx context.Context

	// All defined conditions must match for the rule to fire (AND).

	//===============Plan Specific Conditions================
	// Any matched plan will make this condition true (OR)
	plans []planbuilder.PlanType
	// Any matched fullyQualifiedTableNames will make this condition true (OR)
	fullyQualifiedTableNames []string
	// Regexp conditions. nil conditions are ignored (TRUE).
	query namedRegexp
	// queryTemplate is the query template that will be used to match against the query
	queryTemplate string

	//===============Execution Specific Conditions================
	// Regexp conditions. nil conditions are ignored (TRUE).
	requestIP, user, leadingComment, trailingComment namedRegexp
	// All BindVar conditions have to be fulfilled to make this true (AND)
	bindVarConds []BindVarCond

	// Action to be performed on trigger
	act Action

	actionArgs string
}

type namedRegexp struct {
	name string
	*regexp.Regexp
}

// MarshalJSON marshals to JSON.
func (nr namedRegexp) MarshalJSON() ([]byte, error) {
	return json.Marshal(nr.name)
}

// Equal returns true if other is equal to this namedRegexp, otherwise false.
func (nr namedRegexp) Equal(other namedRegexp) bool {
	if nr.Regexp == nil || other.Regexp == nil {
		return nr.Regexp == nil && other.Regexp == nil && nr.name == other.name
	}
	return nr.name == other.name && nr.String() == other.String()
}

func (nr namedRegexp) String() string {
	return nr.name
}

// NewActiveQueryRule creates a new Rule.
func NewActiveQueryRule(description, name string, act Action) (qr *Rule) {
	// We ignore act because there's only one action right now
	return &Rule{Description: description, Name: name, act: act, Status: Active}
}

// NewActiveBufferedTableQueryRule creates a new buffer Rule.
func NewActiveBufferedTableQueryRule(cancelCtx context.Context, tableName string, description string) (qr *Rule) {
	// We ignore act because there's only one action right now
	return &Rule{
		cancelCtx:                cancelCtx,
		Description:              description,
		Name:                     bufferedTableRuleName,
		fullyQualifiedTableNames: []string{tableName},
		act:                      QRBuffer,
		Status:                   Active,
	}
}

// Equal returns true if other is equal to this Rule, otherwise false.
func (qr *Rule) Equal(other *Rule) bool {
	if qr == nil || other == nil {
		return qr == nil && other == nil
	}
	return (qr.Description == other.Description &&
		qr.Name == other.Name &&
		qr.Priority == other.Priority &&
		qr.Status == other.Status &&
		qr.requestIP.Equal(other.requestIP) &&
		qr.user.Equal(other.user) &&
		qr.query.Equal(other.query) &&
		qr.queryTemplate == other.queryTemplate &&
		qr.leadingComment.Equal(other.leadingComment) &&
		qr.trailingComment.Equal(other.trailingComment) &&
		reflect.DeepEqual(qr.plans, other.plans) &&
		reflect.DeepEqual(qr.fullyQualifiedTableNames, other.fullyQualifiedTableNames) &&
		reflect.DeepEqual(qr.bindVarConds, other.bindVarConds) &&
		qr.act == other.act &&
		qr.actionArgs == other.actionArgs)
}

// Copy performs a deep copy of a Rule.
func (qr *Rule) Copy() (newqr *Rule) {
	newqr = &Rule{
		Description:     qr.Description,
		Name:            qr.Name,
		Priority:        qr.Priority,
		Status:          qr.Status,
		requestIP:       qr.requestIP,
		user:            qr.user,
		query:           qr.query,
		queryTemplate:   qr.queryTemplate,
		leadingComment:  qr.leadingComment,
		trailingComment: qr.trailingComment,
		act:             qr.act,
		actionArgs:      qr.actionArgs,
		cancelCtx:       qr.cancelCtx,
	}
	if qr.plans != nil {
		newqr.plans = make([]planbuilder.PlanType, len(qr.plans))
		copy(newqr.plans, qr.plans)
	}
	if qr.fullyQualifiedTableNames != nil {
		newqr.fullyQualifiedTableNames = make([]string, len(qr.fullyQualifiedTableNames))
		copy(newqr.fullyQualifiedTableNames, qr.fullyQualifiedTableNames)
	}
	if qr.bindVarConds != nil {
		newqr.bindVarConds = make([]BindVarCond, len(qr.bindVarConds))
		copy(newqr.bindVarConds, qr.bindVarConds)
	}
	return newqr
}

// MarshalJSON marshals to JSON.
func (qr *Rule) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	safeEncode(b, `{"Description":`, qr.Description)
	safeEncode(b, `,"Name":`, qr.Name)
	safeEncode(b, `,"Priority":`, qr.Priority)
	safeEncode(b, `,"Status":`, qr.Status)
	if qr.requestIP.Regexp != nil {
		safeEncode(b, `,"RequestIP":`, qr.requestIP)
	}
	if qr.user.Regexp != nil {
		safeEncode(b, `,"User":`, qr.user)
	}
	if qr.query.Regexp != nil {
		safeEncode(b, `,"Query":`, qr.query)
	}
	safeEncode(b, `,"QueryTemplate":`, qr.queryTemplate)
	if qr.leadingComment.Regexp != nil {
		safeEncode(b, `,"LeadingComment":`, qr.leadingComment)
	}
	if qr.trailingComment.Regexp != nil {
		safeEncode(b, `,"TrailingComment":`, qr.trailingComment)
	}
	if qr.plans != nil {
		safeEncode(b, `,"Plans":`, qr.plans)
	}
	if qr.fullyQualifiedTableNames != nil {
		safeEncode(b, `,"FullyQualifiedTableNames":`, qr.fullyQualifiedTableNames)
	}
	if qr.bindVarConds != nil {
		safeEncode(b, `,"BindVarConds":`, qr.bindVarConds)
	}
	if qr.act != QRContinue {
		safeEncode(b, `,"Action":`, qr.act)
	}
	safeEncode(b, `,"ActionArgs":`, qr.actionArgs)
	_, _ = b.WriteString("}")
	return b.Bytes(), nil
}

// ToBindVariable returns a BindVariable representation of the Rule.
func (qr *Rule) ToBindVariable() (map[string]*querypb.BindVariable, error) {
	bindVars := map[string]*querypb.BindVariable{
		"name":                   sqltypes.StringBindVariable(qr.Name),
		"description":            sqltypes.StringBindVariable(qr.Description),
		"priority":               sqltypes.Int64BindVariable(int64(qr.Priority)),
		"status":                 sqltypes.StringBindVariable(qr.Status),
		"query_regex":            sqltypes.StringBindVariable(qr.query.String()),
		"query_template":         sqltypes.StringBindVariable(qr.queryTemplate),
		"request_ip_regex":       sqltypes.StringBindVariable(qr.requestIP.String()),
		"user_regex":             sqltypes.StringBindVariable(qr.user.String()),
		"leading_comment_regex":  sqltypes.StringBindVariable(qr.leadingComment.String()),
		"trailing_comment_regex": sqltypes.StringBindVariable(qr.trailingComment.String()),
		"action":                 sqltypes.StringBindVariable(qr.act.String()),
		"action_args":            sqltypes.StringBindVariable(qr.actionArgs),
	}
	if qr.plans != nil {
		planStrings, err := json.Marshal(qr.plans)
		if err != nil {
			log.Errorf("Failed to marshal plans: %v", err)
			return nil, err
		}
		bindVars["plans"] = sqltypes.StringBindVariable(string(planStrings))
	} else {
		bindVars["plans"] = sqltypes.StringBindVariable("")
	}

	if qr.fullyQualifiedTableNames != nil {
		tableNames, err := json.Marshal(qr.fullyQualifiedTableNames)
		if err != nil {
			log.Errorf("Failed to marshal fully_qualified_table_names: %v", err)
			return nil, err
		}
		bindVars["fully_qualified_table_names"] = sqltypes.StringBindVariable(string(tableNames))
	} else {
		bindVars["fully_qualified_table_names"] = sqltypes.StringBindVariable("")
	}
	if qr.bindVarConds != nil {
		bindVarConds, err := json.Marshal(qr.bindVarConds)
		if err != nil {
			log.Errorf("Failed to marshal bind_var_conds: %v", err)
			return nil, err
		}
		bindVars["bind_var_conds"] = sqltypes.StringBindVariable(string(bindVarConds))
	} else {
		bindVars["bind_var_conds"] = sqltypes.StringBindVariable("")
	}
	return bindVars, nil
}

// SetPriority sets the priority of the rule.
func (qr *Rule) SetPriority(priority int) {
	qr.Priority = priority
}

// SetStatus sets the status of the rule.
func (qr *Rule) SetStatus(status string) {
	qr.Status = status
}

// SetQueryTemplate sets the query template of the rule.
func (qr *Rule) SetQueryTemplate(queryTemplate string) {
	qr.queryTemplate = queryTemplate
}

// SetAction sets the action of the rule.
func (qr *Rule) SetAction(act Action) {
	qr.act = act
}

// SetActionArgs sets the action arguments of the rule.
func (qr *Rule) SetActionArgs(actionArgs string) {
	qr.actionArgs = actionArgs
}

// SetIPCond adds a regular expression condition for the client IP.
// It has to be a full match (not substring).
func (qr *Rule) SetIPCond(pattern string) (err error) {
	qr.requestIP.name = pattern
	qr.requestIP.Regexp, err = regexp.Compile(makeExact(pattern))
	return err
}

// SetUserCond adds a regular expression condition for the user name
// used by the client.
func (qr *Rule) SetUserCond(pattern string) (err error) {
	qr.user.name = pattern
	qr.user.Regexp, err = regexp.Compile(makeExact(pattern))
	return
}

// AddPlanCond adds to the list of plans that can be matched for
// the rule to fire.
// This function acts as an OR: Any plan id match is considered a match.
func (qr *Rule) AddPlanCond(planType planbuilder.PlanType) {
	qr.plans = append(qr.plans, planType)
}

// AddTableCond adds to the list of fullyQualifiedTableNames that can be matched for
// the rule to fire.
// This function acts as an OR: Any tableName match is considered a match.
func (qr *Rule) AddTableCond(tableName string) {
	qr.fullyQualifiedTableNames = append(qr.fullyQualifiedTableNames, tableName)
}

// SetQueryCond adds a regular expression condition for the query.
func (qr *Rule) SetQueryCond(pattern string) (err error) {
	qr.query.name = pattern
	qr.query.Regexp, err = regexp.Compile(makeExact(pattern))
	return
}

// SetLeadingCommentCond adds a regular expression condition for a leading query comment.
func (qr *Rule) SetLeadingCommentCond(pattern string) (err error) {
	qr.leadingComment.name = pattern
	qr.leadingComment.Regexp, err = regexp.Compile(makeExact(pattern))
	return
}

// SetTrailingCommentCond adds a regular expression condition for a trailing query comment.
func (qr *Rule) SetTrailingCommentCond(pattern string) (err error) {
	qr.trailingComment.name = pattern
	qr.trailingComment.Regexp, err = regexp.Compile(makeExact(pattern))
	return
}

// makeExact forces a full string match for the regex instead of substring
func makeExact(pattern string) string {
	return fmt.Sprintf("^%s$", pattern)
}

// AddBindVarCond adds a bind variable restriction to the Rule.
// All bind var conditions have to be satisfied for the Rule
// to be a match.
// name represents the name (not regexp) of the bind variable.
// onAbsent specifies the value of the condition if the
// bind variable is absent.
// onMismatch specifies the value of the condition if there's
// a type mismatch on the condition.
// For inequalities, the bindvar is the left operand and the value
// in the condition is the right operand: bindVar Operator value.
// Value & operator rules
// Type     Operators                              Bindvar
// nil      ""                                     any type
// uint64   ==, !=, <, >=, >, <=                   whole numbers
// int64    ==, !=, <, >=, >, <=                   whole numbers
// string   ==, !=, <, >=, >, <=, MATCH, NOMATCH   []byte, string
// whole numbers can be: int, int8, int16, int32, int64, uint64
func (qr *Rule) AddBindVarCond(name string, onAbsent, onMismatch bool, op Operator, value any) error {
	var converted bvcValue
	if op == QRNoOp {
		qr.bindVarConds = append(qr.bindVarConds, BindVarCond{name, onAbsent, onMismatch, op, nil})
		return nil
	}
	switch v := value.(type) {
	case uint64:
		if op < QREqual || op > QRLessEqual {
			goto Error
		}
		converted = bvcuint64(v)
	case int64:
		if op < QREqual || op > QRLessEqual {
			goto Error
		}
		converted = bvcint64(v)
	case string:
		if op >= QREqual && op <= QRLessEqual {
			converted = bvcstring(v)
		} else if op >= QRMatch && op <= QRNoMatch {
			var err error
			// Change the value to compiled regexp
			re, err := regexp.Compile(makeExact(v))
			if err != nil {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "processing %s: %v", v, err)
			}
			converted = bvcre{re}
		} else {
			goto Error
		}
	default:
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "type %T not allowed as condition operand (%v)", value, value)
	}
	qr.bindVarConds = append(qr.bindVarConds, BindVarCond{name, onAbsent, onMismatch, op, converted})
	return nil

Error:
	return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid operator %v for type %T (%v)", op, value, value)
}

// FilterByPlan returns a new Rule if the query and planid match.
// The new Rule will contain all the original constraints other
// than the plan and query. If the plan and query don't match the Rule,
// then it returns nil.
func (qr *Rule) FilterByPlan(query string, planType planbuilder.PlanType, tableNames []string) (newqr *Rule) {
	if qr.Status == InActive {
		return nil
	}
	if !planMatch(qr.plans, planType) {
		return nil
	}
	if !fullyQualifiedTableNameRegexMatch(qr.fullyQualifiedTableNames, tableNames) {
		return nil
	}
	if !reMatch(qr.query.Regexp, query) {
		return nil
	}
	if !queryTemplateMatch(qr.queryTemplate, query) {
		return nil
	}
	newqr = qr.Copy()
	newqr.query = namedRegexp{}
	// Note we explicitly don't remove the leading/trailing comments as they
	// must be evaluated at execution time.
	newqr.plans = nil
	newqr.fullyQualifiedTableNames = nil
	return newqr
}

// GetAction returns the action for a single rule.
func (qr *Rule) GetAction(
	ip,
	user string,
	bindVars map[string]*querypb.BindVariable,
	marginComments sqlparser.MarginComments,
) Action {
	if qr.cancelCtx != nil {
		select {
		case <-qr.cancelCtx.Done():
			// rule was cancelled. Nothing else to check
			return QRContinue
		default:
			// rule will be cancelled in the future. Until then, it applies!
			// proceed to evaluate rules
		}
	}
	if !reMatch(qr.user.Regexp, user) {
		return QRContinue
	}
	if !reMatch(qr.requestIP.Regexp, ip) {
		return QRContinue
	}
	if !reMatch(qr.leadingComment.Regexp, marginComments.Leading) {
		return QRContinue
	}
	if !reMatch(qr.trailingComment.Regexp, marginComments.Trailing) {
		return QRContinue
	}
	for _, bvcond := range qr.bindVarConds {
		if !bvMatch(bvcond, bindVars) {
			return QRContinue
		}
	}
	return qr.act
}

func (qr *Rule) FilterByExecutionInfo(
	ip,
	user string,
	bindVars map[string]*querypb.BindVariable,
	marginComments sqlparser.MarginComments,
) Action {
	if !reMatch(qr.user.Regexp, user) {
		return QRContinue
	}
	if !reMatch(qr.requestIP.Regexp, ip) {
		return QRContinue
	}
	if !reMatch(qr.leadingComment.Regexp, marginComments.Leading) {
		return QRContinue
	}
	if !reMatch(qr.trailingComment.Regexp, marginComments.Trailing) {
		return QRContinue
	}
	for _, bvcond := range qr.bindVarConds {
		if !bvMatch(bvcond, bindVars) {
			return QRContinue
		}
	}
	return qr.act
}

func reMatch(re *regexp.Regexp, val string) bool {
	return re == nil || re.String() == "^$" || re.MatchString(val)
}

func queryTemplateMatch(expect string, actual string) bool {
	return expect == "" || expect == actual
}

func planMatch(plans []planbuilder.PlanType, plan planbuilder.PlanType) bool {
	if plans == nil {
		return true
	}
	for _, p := range plans {
		if p == plan {
			return true
		}
	}
	return false
}

func compileRegex(pattern string) (*regexp.Regexp, error) {
	regexPattern := strings.Replace(pattern, ".", "\\.", -1)
	regexPattern = strings.Replace(regexPattern, "*", ".*", -1)

	re, err := regexp.Compile("^" + regexPattern + "$")
	if err != nil {
		log.Errorf("Invalid pattern %v, err is %v", pattern, err)
		return nil, err
	}
	return re, nil
}

// expectedFullyQualifiedTableNames is a list of fully qualified table names that consist of regex.
// fullyQualifiedTableNames is a list of real table name.
// the caller should guarantee that regex CAN NOT CONTAIN '.', or it will be confounded with the '.' between database name and table name.
func fullyQualifiedTableNameRegexMatch(expectedFullyQualifiedTableNames []string, fullyQualifiedTableNames []string) bool {
	if expectedFullyQualifiedTableNames == nil {
		return true
	}

	for _, expected := range expectedFullyQualifiedTableNames {
		expectedParts := strings.Split(expected, ".")
		if len(expectedParts) != 2 {
			log.Errorf("expectedFullyQualifiedTableNames is not fully qualified table name, expected:%v", expected)
			return false
		}
		databaseNameRegex, err1 := compileRegex(expectedParts[0])
		tableNameRegex, err2 := compileRegex(expectedParts[1])
		if err1 != nil || err2 != nil {
			log.Errorf("err of compileRegex is not nil, err1:%v, err2:%v", err1, err2)
			return false
		}
		for _, actual := range fullyQualifiedTableNames {
			actualParts := strings.Split(actual, ".")
			if len(actualParts) != 2 {
				log.Errorf("fullyQualifiedTableNames is not fully qualified table name, actual:%v", actual)
				return false
			}
			databaseMatched := databaseNameRegex.MatchString(actualParts[0])
			tableMatched := tableNameRegex.MatchString(actualParts[1])
			if databaseMatched && tableMatched {
				return true
			}
		}
	}
	return false
}

func bvMatch(bvcond BindVarCond, bindVars map[string]*querypb.BindVariable) bool {
	bv, ok := bindVars[bvcond.name]
	if !ok {
		return bvcond.onAbsent
	}
	if bvcond.op == QRNoOp {
		return !bvcond.onAbsent
	}
	return bvcond.value.eval(bv, bvcond.op, bvcond.onMismatch)
}

//-----------------------------------------------
// Support types for Rule

// Action speficies the list of actions to perform
// when a Rule is triggered.
type Action int

// These are actions.
const (
	QRContinue = Action(iota)
	QRFail
	QRFailRetry
	QRBuffer
	QRConcurrencyControl
	QRPlugin
)

func ParseStringToAction(s string) (Action, error) {
	switch s {
	case "CONTINUE":
		return QRContinue, nil
	case "FAIL":
		return QRFail, nil
	case "FAIL_RETRY":
		return QRFailRetry, nil
	case "BUFFER":
		return QRBuffer, nil
	case "CONCURRENCY_CONTROL":
		return QRConcurrencyControl, nil
	case "PLUGIN":
		return QRPlugin, nil
	default:
		return QRContinue, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid Action %s", s)
	}
}

func (act Action) ToString() string {
	switch act {
	case QRContinue:
		return "CONTINUE"
	case QRFail:
		return "FAIL"
	case QRFailRetry:
		return "FAIL_RETRY"
	case QRBuffer:
		return "BUFFER"
	case QRConcurrencyControl:
		return "CONCURRENCY_CONTROL"
	case QRPlugin:
		return "PLUGIN"
	default:
		return "INVALID"
	}
}

func (act Action) String() string {
	return act.ToString()
}

// MarshalJSON marshals to JSON.
func (act Action) MarshalJSON() ([]byte, error) {
	// If we add more actions, we'll need to use a map.
	return json.Marshal(act.ToString())
}

const (
	Active   = "ACTIVE"
	InActive = "INACTIVE"
)

func StatusIsValid(status string) bool {
	switch status {
	case Active, InActive:
		return true
	}
	return false
}

func IsValidFullyQualifiedTableName(tableName string) bool {
	parts := strings.Split(tableName, ".")
	return len(parts) == 2
}

// BindVarCond represents a bind var condition.
type BindVarCond struct {
	name       string
	onAbsent   bool
	onMismatch bool
	op         Operator
	value      bvcValue
}

// MarshalJSON marshals to JSON.
func (bvc BindVarCond) MarshalJSON() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	safeEncode(b, `{"Name":`, bvc.name)
	safeEncode(b, `,"OnAbsent":`, bvc.onAbsent)
	if bvc.op != QRNoOp {
		safeEncode(b, `,"OnMismatch":`, bvc.onMismatch)
	}
	safeEncode(b, `,"Operator":`, bvc.op)
	if bvc.op != QRNoOp {
		safeEncode(b, `,"Value":`, bvc.value)
	}
	_, _ = b.WriteString("}")
	return b.Bytes(), nil
}

// Operator represents the list of operators.
type Operator int

// These are comparison operators.
const (
	QRNoOp = Operator(iota)
	QREqual
	QRNotEqual
	QRLessThan
	QRGreaterEqual
	QRGreaterThan
	QRLessEqual
	QRMatch
	QRNoMatch
	QRNumOp
)

var opmap = map[string]Operator{
	"":        QRNoOp,
	"==":      QREqual,
	"!=":      QRNotEqual,
	"<":       QRLessThan,
	">=":      QRGreaterEqual,
	">":       QRGreaterThan,
	"<=":      QRLessEqual,
	"MATCH":   QRMatch,
	"NOMATCH": QRNoMatch,
}

var opnames []string

func init() {
	opnames = make([]string, QRNumOp)
	for k, v := range opmap {
		opnames[v] = k
	}
}

// These are return statii.
const (
	QROK = iota
	QRMismatch
	QROutOfRange
)

// MarshalJSON marshals to JSON.
func (op Operator) MarshalJSON() ([]byte, error) {
	return json.Marshal(opnames[op])
}

// bvcValue defines the common interface
// for all bind var condition values
type bvcValue interface {
	eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool
}

type bvcuint64 uint64

func (uval bvcuint64) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	num, status := getuint64(bv)
	switch op {
	case QREqual:
		switch status {
		case QROK:
			return num == uint64(uval)
		case QROutOfRange:
			return false
		}
	case QRNotEqual:
		switch status {
		case QROK:
			return num != uint64(uval)
		case QROutOfRange:
			return true
		}
	case QRLessThan:
		switch status {
		case QROK:
			return num < uint64(uval)
		case QROutOfRange:
			return true
		}
	case QRGreaterEqual:
		switch status {
		case QROK:
			return num >= uint64(uval)
		case QROutOfRange:
			return false
		}
	case QRGreaterThan:
		switch status {
		case QROK:
			return num > uint64(uval)
		case QROutOfRange:
			return false
		}
	case QRLessEqual:
		switch status {
		case QROK:
			return num <= uint64(uval)
		case QROutOfRange:
			return true
		}
	default:
		panic("unreachable")
	}

	return onMismatch
}

type bvcint64 int64

func (ival bvcint64) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	num, status := getint64(bv)
	switch op {
	case QREqual:
		switch status {
		case QROK:
			return num == int64(ival)
		case QROutOfRange:
			return false
		}
	case QRNotEqual:
		switch status {
		case QROK:
			return num != int64(ival)
		case QROutOfRange:
			return true
		}
	case QRLessThan:
		switch status {
		case QROK:
			return num < int64(ival)
		case QROutOfRange:
			return false
		}
	case QRGreaterEqual:
		switch status {
		case QROK:
			return num >= int64(ival)
		case QROutOfRange:
			return true
		}
	case QRGreaterThan:
		switch status {
		case QROK:
			return num > int64(ival)
		case QROutOfRange:
			return true
		}
	case QRLessEqual:
		switch status {
		case QROK:
			return num <= int64(ival)
		case QROutOfRange:
			return false
		}
	default:
		panic("unreachable")
	}

	return onMismatch
}

type bvcstring string

func (sval bvcstring) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	str, status := getstring(bv)
	if status != QROK {
		return onMismatch
	}
	switch op {
	case QREqual:
		return str == string(sval)
	case QRNotEqual:
		return str != string(sval)
	case QRLessThan:
		return str < string(sval)
	case QRGreaterEqual:
		return str >= string(sval)
	case QRGreaterThan:
		return str > string(sval)
	case QRLessEqual:
		return str <= string(sval)
	}
	panic("unreachable")
}

type bvcre struct {
	re *regexp.Regexp
}

func (reval bvcre) eval(bv *querypb.BindVariable, op Operator, onMismatch bool) bool {
	str, status := getstring(bv)
	if status != QROK {
		return onMismatch
	}
	switch op {
	case QRMatch:
		return reval.re.MatchString(str)
	case QRNoMatch:
		return !reval.re.MatchString(str)
	}
	panic("unreachable")
}

// getuint64 returns QROutOfRange for negative values
func getuint64(val *querypb.BindVariable) (uv uint64, status int) {
	bv, err := sqltypes.BindVariableToValue(val)
	if err != nil {
		return 0, QROutOfRange
	}
	v, err := evalengine.ToUint64(bv)
	if err != nil {
		return 0, QROutOfRange
	}
	return v, QROK
}

// getint64 returns QROutOfRange if a uint64 is too large
func getint64(val *querypb.BindVariable) (iv int64, status int) {
	bv, err := sqltypes.BindVariableToValue(val)
	if err != nil {
		return 0, QROutOfRange
	}
	v, err := evalengine.ToInt64(bv)
	if err != nil {
		return 0, QROutOfRange
	}
	return v, QROK
}

// TODO(sougou): this is inefficient. Optimize to use []byte.
func getstring(val *querypb.BindVariable) (s string, status int) {
	if sqltypes.IsIntegral(val.Type) || sqltypes.IsFloat(val.Type) || sqltypes.IsText(val.Type) || sqltypes.IsBinary(val.Type) {
		return string(val.Value), QROK
	}
	return "", QRMismatch
}

//-----------------------------------------------
// Support functions for JSON

// MapStrOperator maps a string representation to an Operator.
func MapStrOperator(strop string) (op Operator, err error) {
	if op, ok := opmap[strop]; ok {
		return op, nil
	}
	return QRNoOp, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid Operator %s", strop)
}

// BuildQueryRule builds a query rule from a ruleInfo.
func BuildQueryRule(ruleInfo map[string]any) (qr *Rule, err error) {
	qr = NewActiveQueryRule("", "", QRFail)
	for k, v := range ruleInfo {
		var sv string
		var iv int
		var lv []any
		var ok bool
		switch k {
		case "Name", "Description", "RequestIP", "User", "Query",
			"Action", "LeadingComment", "TrailingComment", "Status",
			"QueryTemplate", "ActionArgs":
			sv, ok = v.(string)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for %s", k)
			}
		case "Priority":
			// if v is json.Number, convert it to int
			if num, ok := v.(json.Number); ok {
				intNum, err := num.Int64()
				if err != nil {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want int for Priority")
				}
				v = int(intNum)
			}
			iv, ok = v.(int)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want int for Priority")
			}
			if iv < MinPriority {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Priority must be >= %d", MinPriority)
			}
		case "Plans", "BindVarConds", "FullyQualifiedTableNames":
			lv, ok = v.([]any)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want list for %s", k)
			}
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unrecognized tag %s", k)
		}
		switch k {
		case "Name":
			qr.Name = sv
		case "Priority":
			qr.Priority = iv
		case "Status":
			if !StatusIsValid(sv) {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid status: %s", sv)
			}
			qr.Status = sv
		case "Description":
			qr.Description = sv
		case "RequestIP":
			err = qr.SetIPCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set IP condition: %v", sv)
			}
		case "User":
			err = qr.SetUserCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set User condition: %v", sv)
			}
		case "Query":
			err = qr.SetQueryCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set Query condition: %v", sv)
			}
		case "QueryTemplate":
			qr.queryTemplate = sv
		case "LeadingComment":
			err = qr.SetLeadingCommentCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set LeadingComment condition: %v", sv)
			}
		case "TrailingComment":
			err = qr.SetTrailingCommentCond(sv)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "could not set TrailingComment condition: %v", sv)
			}
		case "Plans":
			for _, p := range lv {
				pv, ok := p.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for Plans")
				}
				pt, ok := planbuilder.PlanByName(pv)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid plan name: %s", pv)
				}
				qr.AddPlanCond(pt)
			}
		case "FullyQualifiedTableNames":
			for _, t := range lv {
				fullyQualifiedTableName, ok := t.(string)
				if !ok {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for fullyQualifiedTableName")
				}
				if !IsValidFullyQualifiedTableName(fullyQualifiedTableName) {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "fullyQualifiedTableName %v is not in correct format, should be databaseName.tableName", fullyQualifiedTableName)
				}
				qr.AddTableCond(fullyQualifiedTableName)
			}
		case "BindVarConds":
			for _, bvc := range lv {
				name, onAbsent, onMismatch, op, value, err := buildBindVarCondition(bvc)
				if err != nil {
					return nil, err
				}
				err = qr.AddBindVarCond(name, onAbsent, onMismatch, op, value)
				if err != nil {
					return nil, err
				}
			}
		case "Action":
			act, err := ParseStringToAction(sv)
			if err != nil {
				return nil, err
			}
			qr.act = act
		case "ActionArgs":
			qr.actionArgs = sv
		}
	}
	return qr, nil
}

func buildBindVarCondition(bvc any) (name string, onAbsent, onMismatch bool, op Operator, value any, err error) {
	bvcinfo, ok := bvc.(map[string]any)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want json object for bind var conditions")
		return
	}

	var v any
	v, ok = bvcinfo["Name"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Name missing in BindVarConds")
		return
	}
	name, ok = v.(string)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for Name in BindVarConds")
		return
	}

	v, ok = bvcinfo["OnAbsent"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "OnAbsent missing in BindVarConds")
		return
	}
	onAbsent, ok = v.(bool)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want bool for OnAbsent")
		return
	}

	v, ok = bvcinfo["Operator"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Operator missing in BindVarConds")
		return
	}
	strop, ok := v.(string)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string for Operator")
		return
	}
	op, err = MapStrOperator(strop)
	if err != nil {
		return
	}
	if op == QRNoOp {
		return
	}
	v, ok = bvcinfo["Value"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Value missing in BindVarConds")
		return
	}
	if op >= QREqual && op <= QRLessEqual {
		switch v := v.(type) {
		case json.Number:
			value, err = v.Int64()
			if err != nil {
				// Maybe uint64
				value, err = strconv.ParseUint(string(v), 10, 64)
				if err != nil {
					err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want int64/uint64: %s", string(v))
					return
				}
			}
		case string:
			value = v
		default:
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string or number: %v", v)
			return
		}
	} else if op == QRMatch || op == QRNoMatch {
		strvalue, ok := v.(string)
		if !ok {
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want string: %v", v)
			return
		}
		value = strvalue
	}

	v, ok = bvcinfo["OnMismatch"]
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "OnMismatch missing in BindVarConds")
		return
	}
	onMismatch, ok = v.(bool)
	if !ok {
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "want bool for OnMismatch")
		return
	}
	return
}

func safeEncode(b *bytes.Buffer, prefix string, v any) {
	enc := json.NewEncoder(b)
	_, _ = b.WriteString(prefix)
	if err := enc.Encode(v); err != nil {
		_ = enc.Encode(err.Error())
	}
}

// GetCancelCtx returns the cancel context for the rule
func (qr *Rule) GetCancelCtx() context.Context {
	return qr.cancelCtx
}

// GetActionArgs
func (qr *Rule) GetActionArgs() string {
	return qr.actionArgs
}

// GetActionType
func (qr *Rule) GetActionType() string {
	return qr.act.ToString()
}
