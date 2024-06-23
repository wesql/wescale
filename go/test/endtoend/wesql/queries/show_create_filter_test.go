package queries

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
)

func TestShowCreateFilter(t *testing.T) {
	execWithConnWithoutDB(t, func(conn *mysql.Conn) {

		createSQL1 := "\ncreate filter ccl (\n\tdesc='test ccl',\n\tpriority='999',\n\tstatus='ACTIVE'\n)\nwith_pattern (\n\tplans='Select',\n\tfully_qualified_table_names='d1.t1',\n\tquery_regex='',\n\tquery_template='',\n\trequest_ip_regex='',\n\tuser_regex='',\n\tleading_comment_regex='',\n\ttrailing_comment_regex='',\n\tbind_var_conds=''\n)\nexecute (\n\taction='CONCURRENCY_CONTROL',\n\taction_args='max_queue_size=0;max_concurrency=0'\n)"
		expectedSQL1 := createSQL1
		_, err := conn.ExecuteFetch(createSQL1, 1000, true)
		assert.Equal(t, nil, err)
		showCreateFilterCCL := `show create filter ccl;`
		qr, err := conn.ExecuteFetch(showCreateFilterCCL, 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(qr.Named().Rows))
		got := qr.Named().Rows[0]["Create Filter"].ToString()
		fmt.Printf("got: %s\n", got)
		assert.Equal(t, expectedSQL1, got)

		alterSQL1 := `alter filter ccl ( desc='test ccl', priority='999', status='ACTIVE') with_pattern ( plans='insert,update', fully_qualified_table_names='d1.t1,d2.*,*.t3', query_regex='.*', query_template='.*', request_ip_regex='.*', user_regex='.*', leading_comment_regex='.*', trailing_comment_regex='.*', bind_var_conds='' ) execute ( action='FAIL', action_args='max_queue_size=0;max_concurrency=0' )`
		expectedSQL2 := "\ncreate filter ccl (\n\tdesc='test ccl',\n\tpriority='999',\n\tstatus='ACTIVE'\n)\nwith_pattern (\n\tplans='Insert,Update',\n\tfully_qualified_table_names='d1.t1,d2.*,*.t3',\n\tquery_regex='.*',\n\tquery_template='.*',\n\trequest_ip_regex='.*',\n\tuser_regex='.*',\n\tleading_comment_regex='.*',\n\ttrailing_comment_regex='.*',\n\tbind_var_conds=''\n)\nexecute (\n\taction='FAIL',\n\taction_args=''\n)"
		_, err = conn.ExecuteFetch(alterSQL1, 1000, true)
		// action FAIL does not support action args
		assert.NotNil(t, err)

		alterSQL1 = `alter filter ccl ( desc='test ccl', priority='999', status='ACTIVE') with_pattern ( plans='insert,update', fully_qualified_table_names='d1.t1,d2.*,*.t3', query_regex='.*', query_template='.*', request_ip_regex='.*', user_regex='.*', leading_comment_regex='.*', trailing_comment_regex='.*', bind_var_conds='' ) execute ( action='FAIL', action_args='' )`
		_, err = conn.ExecuteFetch(alterSQL1, 1000, true)
		assert.Nil(t, err)

		qr, err = conn.ExecuteFetch(showCreateFilterCCL, 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(qr.Named().Rows))
		got = qr.Named().Rows[0]["Create Filter"].ToString()
		fmt.Printf("got: %s\n", got)
		assert.Equal(t, expectedSQL2, got)

		alterSQL2 := `alter filter ccl with_pattern ( plans='', fully_qualified_table_names='', query_regex='select \* .*', query_template='', request_ip_regex='127\.0\.0\..*', user_regex='.*', leading_comment_regex='haha.*', trailing_comment_regex='lala.*', bind_var_conds='' )`
		expectedSQL3 := "\ncreate filter ccl (\n\tdesc='test ccl',\n\tpriority='999',\n\tstatus='ACTIVE'\n)\nwith_pattern (\n\tplans='',\n\tfully_qualified_table_names='',\n\tquery_regex='select \\* .*',\n\tquery_template='',\n\trequest_ip_regex='127\\.0\\.0\\..*',\n\tuser_regex='.*',\n\tleading_comment_regex='haha.*',\n\ttrailing_comment_regex='lala.*',\n\tbind_var_conds=''\n)\nexecute (\n\taction='FAIL',\n\taction_args=''\n)"
		_, err = conn.ExecuteFetch(alterSQL2, 1000, true)
		assert.Equal(t, nil, err)
		qr, err = conn.ExecuteFetch(showCreateFilterCCL, 1000, true)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(qr.Named().Rows))
		got = qr.Named().Rows[0]["Create Filter"].ToString()
		fmt.Printf("got: %s\n", got)
		assert.Equal(t, expectedSQL3, got)

	})
}
