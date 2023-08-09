package main

type Column struct {
	Name       string
	Type       string
	IsNullable bool
	IsPrimary  bool
	IsForeign  bool
}

func NewColumn(name_ string, type_ string, isnullable_ bool) *Column {
	return &Column{Name: name_, Type: type_, IsNullable: isnullable_, IsPrimary: false, IsForeign: false}
}

