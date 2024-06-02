package tabletserver

import (
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
)

func (qe *QueryEngine) InitBuiltInFilter() {
	allowList := getBuiltInAllowList()
	_, err := qe.HandleCreateFilter(allowList)
	if err != nil {
		log.Errorf("init built-in filter allow list error: %v", err)
	}
}

const (
	AllowListName                     = "allow_list"
	AllowListDescription              = "a built-in filter to help SQLs on certain system tables skip all filters"
	AllowListPriority                 = "0"
	AllowListStatus                   = "ACTIVE"
	AllowListIfNotExi                 = true
	AllowListPlans                    = "Select,Insert,Update,Delete"
	AllowListFullyQualifiedTableNames = "mysql.wasm_binary"
	AllowListQueryRegex               = ""
	AllowListQueryTemplate            = ""
	AllowListRequestIPRegex           = ""
	AllowListUserRegex                = ""
	AllowListLeadingCommentRegex      = ""
	AllowListTrailingCommentRegex     = ""
	AllowListBindVarConds             = ""
	AllowListAction                   = rules.QRSkipFilter
	AllowListActionArgs               = `allow_list=".*"`
)

func getBuiltInAllowList() *sqlparser.CreateWescaleFilter {
	return &sqlparser.CreateWescaleFilter{
		Name:        AllowListName,
		Description: AllowListDescription,
		Priority:    AllowListPriority,
		Status:      AllowListStatus,
		IfNotExists: AllowListIfNotExi,
		Pattern: &sqlparser.WescaleFilterPattern{Plans: AllowListPlans, FullyQualifiedTableNames: AllowListFullyQualifiedTableNames, QueryRegex: AllowListQueryRegex,
			QueryTemplate: AllowListQueryTemplate, RequestIPRegex: AllowListRequestIPRegex, UserRegex: AllowListUserRegex, LeadingCommentRegex: AllowListLeadingCommentRegex,
			TrailingCommentRegex: AllowListTrailingCommentRegex, BindVarConds: AllowListBindVarConds},
		Action: &sqlparser.WescaleFilterAction{Action: AllowListAction.ToString(), ActionArgs: AllowListActionArgs},
	}
}
