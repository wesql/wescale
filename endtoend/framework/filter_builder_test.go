package framework

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFilterBuilder(t *testing.T) {
	sql, err := NewFilterBuilder("trailing_comment_reg").
		SetDescription("test description").
		SetPriority("999").
		SetTrailingCommentRegex("h.*w.*").
		SetAction("FAIL").
		Build()
	assert.NoError(t, err)
	expect := `create filter if not exists trailing_comment_reg (
        desc='test description',
        priority='999',
        status='ACTIVE'
)
with_pattern(
        plans='',
        fully_qualified_table_names='',
        query_regex='',
        query_template='',
        request_ip_regex='',
        user_regex='',
        leading_comment_regex='',
        trailing_comment_regex='h.*w.*',
        bind_var_conds=''
)
execute(
        action='FAIL',
        action_args=''
);`
	assert.Equal(t, expect, sql)
}
