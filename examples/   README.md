# Examples

## Creating a new Wrapper
``` golang
db, err := sql.Open("driver", "DSN")
if err != nil {
	panic(err)
}
defer db.Close()

wrapper := NewWrapper(db)
```

## Unwrap
``` golang
if err = wrapper.Query(`SELECT * FROM TABLE WHERE EXAMPLE`); err != nil {
    panic(err)
}

type table struct {
	ID    int    `json:"id"    sql:"id"`
	Name  string `json:"name"  sql:"name"`
	Email string `json:"email" sql:"email"`
}
var results []table
if err = wrapper.Unwrap(&results); err != nil {
	panic(err)
}
for _, result := range results {
	fmt.Printf("ID:\t%v\nName:\t%v\nEmail:\t%v\n", result.ID, result.Name, result.Email)
}
```

## Unwrap Example 2
``` golang
var results []table
if err = wrapper.Unwrap(&results, func(result *table){
    // This is executed after the result has been unmarshalled to.
}); err != nil {
	panic(err)
}
```

## Unmarshal
``` golang
if err = wrapper.Query(`SELECT * FROM TABLE WHERE EXAMPLE`); err != nil {
	panic(err)
}

type table struct {
	ID    int    `json:"id"    sql:"id"`
	Name  string `json:"name"  sql:"name"`
	Email string `json:"email" sql:"email"`
}
results := make([]table, wrapper.RowCount())

for i := 0; wrapper.Next(); i++ {
	if err = wrapper.Unmarshal(&results[i]); err != nil {
		panic(err)
	}
}

for _, result := range table {
	fmt.Printf("ID:\t%v\nName:\t%v\nEmail:\t%v\n", result.ID, result.Name, result.Email)
}
```

## Getting Variables
``` golang
if err = wrapper.Query(`SELECT * FROM TABLE WHERE EXAMPLE`); err != nil {
	panic(err)
}

for wrapper.Next() {
	fmt.Printf("ID:\t%v\nName:\t%v\nEmail:\t%v\n",
		wrapper.GetInt("id"),
		wrapper.GetString("name"),
		wrapper.GetString("email"),
	)
}
```

## Transactions
This one is still being refined but...
``` golang
if err = wrapper.Begin(); err != nil {
	return
}
// You could probably handle this a lot better though.
defer func() {
    if err != nil {
        if err = wrapper.Revert(); err != nil {
            panic(err)
        }
    } else {
        if err = wrapper.Commit(); err != nil {
            if err = wrapper.Revert(); err != nil {
                panic(err)
            }
        }
    }
}()
```