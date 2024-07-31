package main

import (
	"fmt"
	"github.com/wesql/sqlparser/go/sqltypes"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	"sort"
)

// ColumnInfo is used to store charset and collation
type ColumnInfo struct {
	Name        string
	CharSet     string
	Collation   string
	DataType    string
	ColumnType  string
	IsPK        bool
	IsGenerated bool
	SeqInIndex  int
}

func buildColInfoMap(tableSchema, tableName string, executor func(sql string) (*sqltypes.Result, error)) (map[string]*ColumnInfo, error) {
	colInfoMap := make(map[string]*ColumnInfo)
	query := fmt.Sprintf(`SELECT
            c.COLUMN_NAME,
            c.CHARACTER_SET_NAME,
            c.COLLATION_NAME,
            c.DATA_TYPE,
            c.COLUMN_TYPE,
            c.COLUMN_KEY = 'PRI' AS IS_PK,
            c.EXTRA LIKE '%%GENERATED%%' AS IS_GENERATED,
            IFNULL(s.SEQ_IN_INDEX, 0) AS SEQ_IN_INDEX
        FROM
            information_schema.COLUMNS c
            LEFT JOIN information_schema.STATISTICS s
            ON c.TABLE_SCHEMA = s.TABLE_SCHEMA
            AND c.TABLE_NAME = s.TABLE_NAME
            AND c.COLUMN_NAME = s.COLUMN_NAME
            AND s.INDEX_NAME = 'PRIMARY'
        WHERE
            c.TABLE_SCHEMA = '%s' AND c.TABLE_NAME = '%s'
        ORDER BY
            c.ORDINAL_POSITION
    `, tableSchema, tableName)

	qr, err := executor(query)
	if err != nil {
		return nil, err
	}

	for _, row := range qr.Rows {
		isPk, err := row[5].ToBool()
		if err != nil {
			return nil, err
		}
		isGenerated, err := row[6].ToBool()
		if err != nil {
			return nil, err
		}
		seqInIndex, err := row[7].ToInt64()
		if err != nil {
			return nil, err
		}
		colInfo := &ColumnInfo{
			Name:        row[0].ToString(),
			CharSet:     row[1].ToString(),
			Collation:   row[2].ToString(),
			DataType:    row[3].ToString(),
			ColumnType:  row[4].ToString(),
			IsPK:        isPk,
			IsGenerated: isGenerated,
			SeqInIndex:  int(seqInIndex),
		}
		colInfoMap[colInfo.Name] = colInfo
	}
	return colInfoMap, nil
}

func getPkColumnsOrderBySeqInIndex(colInfoMap map[string]*ColumnInfo) []*ColumnInfo {
	pkColumns := make([]*ColumnInfo, 0)
	for _, colInfo := range colInfoMap {
		if colInfo.IsPK {
			pkColumns = append(pkColumns, colInfo)
		}
	}
	sort.Slice(pkColumns, func(i, j int) bool {
		return pkColumns[i].SeqInIndex < pkColumns[j].SeqInIndex
	})
	return pkColumns
}

func getPkFields(pkColumns []*ColumnInfo, fields []*querypb.Field) []*querypb.Field {
	pkFields := make([]*querypb.Field, 0)
	for _, colInfo := range pkColumns {
		for _, field := range fields {
			if field.Name == colInfo.Name {
				pkFields = append(pkFields, field)
			}
		}
	}
	return pkFields
}
