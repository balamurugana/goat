/*
 * Minio Cloud Storage, (C) 2019 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func newLiteralExpr(parserExpr sqlparser.Expr) (Expr, error) {
	switch parserExpr.(type) {
	case *sqlparser.NullVal:
		return newValueExpr(NewNull()), nil
	case sqlparser.BoolVal:
		return newValueExpr(NewBool((bool(parserExpr.(sqlparser.BoolVal))))), nil
	case *sqlparser.SQLVal:
		sqlValue := parserExpr.(*sqlparser.SQLVal)
		value, err := NewValue(sqlValue)
		if err != nil {
			return nil, err
		}
		return newValueExpr(value), nil
	case *sqlparser.ColName:
		return newColumnExpr(parserExpr.(*sqlparser.ColName)), nil
	case sqlparser.ValTuple:
		var valueType Type
		var values []*Value
		for i, valExpr := range parserExpr.(sqlparser.ValTuple) {
			sqlVal, ok := valExpr.(*sqlparser.SQLVal)
			if !ok {
				return nil, errParseInvalidTypeParam(fmt.Errorf("value %v in Tuple should be primitive value", i+1))
			}

			val, err := NewValue(sqlVal)
			if err != nil {
				return nil, err
			}

			if i == 0 {
				valueType = val.Type()
			} else if valueType != val.Type() {
				return nil, errParseInvalidTypeParam(fmt.Errorf("mixed value type is not allowed in Tuple"))
			}

			values = append(values, val)
		}

		return newValueExpr(NewArray(values)), nil
	}

	return nil, nil
}

func isExprToComparisonExpr(parserExpr *sqlparser.IsExpr, isSelectExpr bool) (Expr, error) {
	leftExpr, err := newExpr(parserExpr.Expr, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newComparisonExpr(ComparisonOperator(parserExpr.Operator), leftExpr)
	if err != nil {
		return nil, err
	}

	if !leftExpr.Type().isBase() {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func rangeCondToComparisonFunc(parserExpr *sqlparser.RangeCond, isSelectExpr bool) (Expr, error) {
	leftExpr, err := newExpr(parserExpr.Left, isSelectExpr)
	if err != nil {
		return nil, err
	}

	fromExpr, err := newExpr(parserExpr.From, isSelectExpr)
	if err != nil {
		return nil, err
	}

	toExpr, err := newExpr(parserExpr.To, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newComparisonExpr(ComparisonOperator(parserExpr.Operator), leftExpr, fromExpr, toExpr)
	if err != nil {
		return nil, err
	}

	if !leftExpr.Type().isBase() || !fromExpr.Type().isBase() || !toExpr.Type().isBase() {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func toComparisonExpr(parserExpr *sqlparser.ComparisonExpr, isSelectExpr bool) (Expr, error) {
	leftExpr, err := newExpr(parserExpr.Left, isSelectExpr)
	if err != nil {
		return nil, err
	}

	rightExpr, err := newExpr(parserExpr.Right, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newComparisonExpr(ComparisonOperator(parserExpr.Operator), leftExpr, rightExpr)
	if err != nil {
		return nil, err
	}

	if !leftExpr.Type().isBase() || !rightExpr.Type().isBase() {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func toArithExpr(parserExpr *sqlparser.BinaryExpr, isSelectExpr bool) (Expr, error) {
	leftExpr, err := newExpr(parserExpr.Left, isSelectExpr)
	if err != nil {
		return nil, err
	}

	rightExpr, err := newExpr(parserExpr.Right, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newArithExpr(ArithOperator(parserExpr.Operator), leftExpr, rightExpr)
	if err != nil {
		return nil, err
	}

	if !leftExpr.Type().isBase() || !rightExpr.Type().isBase() {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func toFuncExpr(parserExpr *sqlparser.FuncExpr, isSelectExpr bool) (Expr, error) {
	funcName := strings.ToUpper(parserExpr.Name.String())
	if !isSelectExpr && isAggregateFuncName(funcName) {
		return nil, errUnsupportedSQLOperation(fmt.Errorf("%v() must be used in select expression", funcName))
	}
	funcs, aggregatedExprFound, err := newSelectExprs(parserExpr.Exprs)
	if err != nil {
		return nil, err
	}

	if aggregatedExprFound {
		return nil, errIncorrectSQLFunctionArgumentType(fmt.Errorf("%v(): aggregated expression must not be used as argument", funcName))
	}

	return newFuncExpr(FuncName(funcName), funcs...)
}

func toAndExpr(parserExpr *sqlparser.AndExpr, isSelectExpr bool) (Expr, error) {
	leftExpr, err := newExpr(parserExpr.Left, isSelectExpr)
	if err != nil {
		return nil, err
	}

	rightExpr, err := newExpr(parserExpr.Right, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newAndExpr(leftExpr, rightExpr)
	if err != nil {
		return nil, err
	}

	if leftExpr.Type() != Bool || rightExpr.Type() != Bool {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func toOrExpr(parserExpr *sqlparser.OrExpr, isSelectExpr bool) (Expr, error) {
	leftExpr, err := newExpr(parserExpr.Left, isSelectExpr)
	if err != nil {
		return nil, err
	}

	rightExpr, err := newExpr(parserExpr.Right, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newOrExpr(leftExpr, rightExpr)
	if err != nil {
		return nil, err
	}

	if leftExpr.Type() != Bool || rightExpr.Type() != Bool {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func toNotExpr(parserExpr *sqlparser.NotExpr, isSelectExpr bool) (Expr, error) {
	rightExpr, err := newExpr(parserExpr.Expr, isSelectExpr)
	if err != nil {
		return nil, err
	}

	f, err := newNotExpr(rightExpr)
	if err != nil {
		return nil, err
	}

	if rightExpr.Type() != Bool {
		return f, nil
	}

	value, err := f.Eval(nil)
	if err != nil {
		return nil, err
	}

	return newValueExpr(value), nil
}

func newExpr(parserExpr sqlparser.Expr, isSelectExpr bool) (Expr, error) {
	f, err := newLiteralExpr(parserExpr)
	if err != nil {
		return nil, err
	}

	if f != nil {
		return f, nil
	}

	switch parserExpr.(type) {
	case *sqlparser.ParenExpr:
		return newExpr(parserExpr.(*sqlparser.ParenExpr).Expr, isSelectExpr)
	case *sqlparser.IsExpr:
		return isExprToComparisonExpr(parserExpr.(*sqlparser.IsExpr), isSelectExpr)
	case *sqlparser.RangeCond:
		return rangeCondToComparisonFunc(parserExpr.(*sqlparser.RangeCond), isSelectExpr)
	case *sqlparser.ComparisonExpr:
		return toComparisonExpr(parserExpr.(*sqlparser.ComparisonExpr), isSelectExpr)
	case *sqlparser.BinaryExpr:
		return toArithExpr(parserExpr.(*sqlparser.BinaryExpr), isSelectExpr)
	case *sqlparser.FuncExpr:
		return toFuncExpr(parserExpr.(*sqlparser.FuncExpr), isSelectExpr)
	case *sqlparser.AndExpr:
		return toAndExpr(parserExpr.(*sqlparser.AndExpr), isSelectExpr)
	case *sqlparser.OrExpr:
		return toOrExpr(parserExpr.(*sqlparser.OrExpr), isSelectExpr)
	case *sqlparser.NotExpr:
		return toNotExpr(parserExpr.(*sqlparser.NotExpr), isSelectExpr)
	}

	return nil, errParseUnsupportedSyntax(fmt.Errorf("unknown expression type %T; %v", parserExpr, parserExpr))
}

func newSelectExprs(parserSelectExprs []sqlparser.SelectExpr) ([]Expr, bool, error) {
	var funcs []Expr
	starExprFound := false
	aggregatedExprFound := false

	for _, selectExpr := range parserSelectExprs {
		switch selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			if starExprFound {
				return nil, false, errParseAsteriskIsNotAloneInSelectList(nil)
			}

			aliasedExpr := selectExpr.(*sqlparser.AliasedExpr)
			f, err := newExpr(aliasedExpr.Expr, true)
			if err != nil {
				return nil, false, err
			}

			if f.Type() == aggregateFunction {
				aggregatedExprFound = true
			} else if aggregatedExprFound {
				return nil, false, errParseUnsupportedSyntax(fmt.Errorf("expression must not mixed with aggregated expression"))
			}

			alias := aliasedExpr.As.String()
			if alias != "" {
				f = newAliasExpr(alias, f)
			}

			funcs = append(funcs, f)
		case *sqlparser.StarExpr:
			if starExprFound {
				err := fmt.Errorf("only single star expression allowed")
				return nil, false, errParseInvalidContextForWildcardInSelectList(err)
			}
			starExprFound = true
			funcs = append(funcs, newStarExpr())
		default:
			return nil, false, errParseUnsupportedSyntax(fmt.Errorf("unknown select expression %v", selectExpr))
		}
	}

	return funcs, aggregatedExprFound, nil
}

// Select - SQL Select statement.
type Select struct {
	tableName           string
	tableAlias          string
	selectExprs         []Expr
	aggregatedExprFound bool
	whereExpr           Expr
}

// TableAlias - returns table alias name.
func (statement *Select) TableAlias() string {
	return statement.tableAlias
}

// AggregateResult - returns aggregate result as record.
func (statement *Select) AggregateResult() (*Record, error) {
	if !statement.aggregatedExprFound {
		return nil, nil
	}

	result := NewRecord(statement.tableAlias)
	for _, expr := range statement.selectExprs {
		value, err := expr.AggregateValue()
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, errInternalError(fmt.Errorf("%v returns <nil> for AggregateValue()", expr))
		}

		name := fmt.Sprintf("%v", expr)
		if _, ok := expr.(*aliasExpr); ok {
			name = expr.(*aliasExpr).alias
		}

		result.Set(name, value)
	}

	return result, nil
}

// Eval - evaluvates this Select expressions for given record.
func (statement *Select) Eval(record *Record) (*Record, error) {
	if statement.whereExpr != nil {
		value, err := statement.whereExpr.Eval(record)
		if err != nil {
			return nil, err
		}

		if value == nil || value.valueType != Bool {
			err = fmt.Errorf("WHERE expression %v returns invalid bool value %v", statement.whereExpr, value)
			return nil, errInternalError(err)
		}

		if !value.BoolValue() {
			return nil, nil
		}
	}

	// Call selectExprs
	result := NewRecord(statement.tableAlias)
	for _, expr := range statement.selectExprs {
		value, err := expr.Eval(record)
		if err != nil {
			return nil, err
		}

		if statement.aggregatedExprFound {
			continue
		}

		name := fmt.Sprintf("%v", expr)
		switch expr.(type) {
		case *starExpr:
			return value.recordValue(), nil
		case *aliasExpr:
			name = expr.(*aliasExpr).alias
		case *columnExpr:
			name = expr.(*columnExpr).name
		}

		result.Set(name, value)
	}

	return result, nil
}

// NewSelect - creates new Select by parsing sql.
func NewSelect(sql string) (*Select, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, errUnsupportedSQLStructure(err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errParseUnsupportedSelect(fmt.Errorf("unsupported SQL statement %v", sql))
	}

	var tableName, tableAlias string
	for _, fromExpr := range selectStmt.From {
		tableExpr := fromExpr.(*sqlparser.AliasedTableExpr)
		tableName = tableExpr.Expr.(sqlparser.TableName).Name.String()
		tableAlias = tableExpr.As.String()
	}

	selectExprs, aggregatedExprFound, err := newSelectExprs(selectStmt.SelectExprs)
	if err != nil {
		return nil, err
	}

	var whereExpr Expr
	if selectStmt.Where != nil {
		whereExpr, err = newExpr(selectStmt.Where.Expr, false)
		if err != nil {
			return nil, err
		}
	}

	return &Select{
		tableName:           tableName,
		tableAlias:          tableAlias,
		selectExprs:         selectExprs,
		aggregatedExprFound: aggregatedExprFound,
		whereExpr:           whereExpr,
	}, nil
}
