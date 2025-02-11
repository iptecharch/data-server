package tree

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/sdcio/data-server/pkg/types"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"github.com/sdcio/yang-parser/xpath"
	"github.com/sdcio/yang-parser/xpath/grammars/expr"
)

func (s *sharedEntryAttributes) validateMustStatements(ctx context.Context, resultChan chan<- *types.ValidationResultEntry) {

	// if no schema, then there is nothing to be done, return
	if s.schema == nil {
		return
	}

	var mustStatements []*sdcpb.MustStatement
	switch schem := s.GetSchema().GetSchema().(type) {
	case *sdcpb.SchemaElem_Container:
		mustStatements = schem.Container.GetMustStatements()
	case *sdcpb.SchemaElem_Leaflist:
		mustStatements = schem.Leaflist.GetMustStatements()
	case *sdcpb.SchemaElem_Field:
		mustStatements = schem.Field.GetMustStatements()
	}

	for _, must := range mustStatements {
		// extract actual must statement
		exprStr := must.Statement
		// init a ProgramBuilder
		prgbuilder := xpath.NewProgBuilder(exprStr)
		// init an ExpressionLexer
		lexer := expr.NewExprLex(exprStr, prgbuilder, nil)
		// parse the provided Must-Expression
		lexer.Parse()
		prog, err := lexer.CreateProgram(exprStr)
		if err != nil {
			resultChan <- types.NewValidationResultEntry(s.leafVariants.GetHighestPrecedence(false, false).Owner(), err, types.ValidationResultEntryTypeError)
			return
		}
		machine := xpath.NewMachine(exprStr, prog, exprStr)

		// run the must statement evaluation virtual machine
		yctx := xpath.NewCtxFromCurrent(ctx, machine, newYangParserEntryAdapter(ctx, s))
		yctx.SetDebug(false)

		res1 := yctx.Run()
		// retrieve the boolean result of the execution
		result, err := res1.GetBoolResult()
		if !result || err != nil {
			if err == nil {
				err = fmt.Errorf("error path: %s, must-statement [%s] %s", must.Statement, s.Path(), must.Error)
			}
			if strings.Contains(err.Error(), "Stack underflow") {
				slog.Debug("stack underflow error: path=%v, mustExpr=%s", s.Path().String(), exprStr)
				continue
			}
			resultChan <- types.NewValidationResultEntry(s.leafVariants.GetHighestPrecedence(false, false).Owner(), err, types.ValidationResultEntryTypeError)
			return
		}
	}
}
