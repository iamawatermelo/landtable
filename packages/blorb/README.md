# blorb

Blorb is a formula language that aims to be (in order):

- **Extensible**
  Blorb should be able to be transpiled to many different languages.

- **Secure**
  Blorb should not result in arbitrary code execution.
  Note that Blorb formulae may take an arbitrary amount of time to execute.
  
- **Feature rich**
  Blorb implements many different operators and functions for ease of use.

- **Fast**
  Blorb aims to be reasonably fast at computing the same formula thousands
  of times.

## Differences from...

### ... Airtable

Blorb supports **lambda functions**, **let bindings** and **pattern matching**.

```
let
    total_doubloons := doubloons * ARRAYREDUCE(bonuses, 0, |acc, v| acc + v)
of
    which (total_doubloons)
        | ..=23 => "You can afford a signed photo of Malted!"
        > "Imagine not being able to afford a signed photo of Malted"
```

### ... Google Sheets

Many things that accept expressions in Google Sheets are keywords in
Blorb, to make it obvious that something special is happening in this case.

- `LET(ident, value, body)` -> `let ident := value of body`
- `LAMBDA(param..., body)` -> `|param...| body`

Blorb doesn't support ranges like Google Sheets does, because it is not
designed to be used as a spreadsheet language. However, it has functions
to operate on arrays. In Blorb, array literals use `[]` square brackets,
not `{}` curly braces, because `{}` denotes a variable that has a space
inside it.