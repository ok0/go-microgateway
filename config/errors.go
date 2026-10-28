package config

import (
	"fmt"
	"slices"
	"strings"
)

// FieldError describes one problem found in a configuration document.
type FieldError struct {
	// Path is the dotted path of the field, such as routes[0].upstream; empty for document-level problems.
	Path string
	// Line is the 1-based source line, or 0 when unknown.
	Line int
	// Msg describes the problem.
	Msg string
}

// String formats the error as "path (line N): msg", omitting the parts that are unknown.
func (e FieldError) String() string {
	switch {
	case e.Path != "" && e.Line > 0:
		return fmt.Sprintf("%s (line %d): %s", e.Path, e.Line, e.Msg)
	case e.Path != "":
		return fmt.Sprintf("%s: %s", e.Path, e.Msg)
	case e.Line > 0:
		return fmt.Sprintf("line %d: %s", e.Line, e.Msg)
	}
	return e.Msg
}

// ValidationError reports every problem found while loading or validating a configuration.
type ValidationError struct {
	// Errors lists the problems, ordered by line and then by path.
	Errors []FieldError
}

// Error summarizes the count of problems and lists each one on its own line.
func (e *ValidationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "invalid configuration: %d error(s)", len(e.Errors))
	for _, fe := range e.Errors {
		b.WriteString("\n  " + fe.String())
	}
	return b.String()
}

func newValidationError(errs []FieldError) *ValidationError {
	slices.SortStableFunc(errs, compareFieldErrors)
	return &ValidationError{Errors: errs}
}

// NewValidationError returns a *ValidationError holding errs, sorted by line and then by path.
func NewValidationError(errs []FieldError) *ValidationError {
	return newValidationError(errs)
}

func compareFieldErrors(a, b FieldError) int {
	if (a.Line > 0) != (b.Line > 0) {
		if a.Line > 0 {
			return -1
		}
		return 1
	}
	if a.Line != b.Line {
		return a.Line - b.Line
	}
	return strings.Compare(a.Path, b.Path)
}
