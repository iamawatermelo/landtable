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
        | .. => "Imagine not being able to afford a signed photo of Malted"
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

## The language

### Datatypes

Currently, there are five:

- number (always 64-bit floating point)
- string
- boolean (true / false)
- empty
- error

### Add 5 to x:

```
x + 5
```

### Map over an array:

```
ARRAYMAP([1, 2, 3, 4], |x| x * 5)
```

### Match on some numbers:

```
which (score)
    | 1000.. => "Well done!"
    | 100..1000 => "Great!"
    | 0!..100 => "Okay!"
    | ..=0 => "Oh dear"
```

> [!NOTE]
> `x..` means value is **greater than or equal to** x
> `x!..` means value is **greater than** x
> `..x` means value is **less than** x
> `..=x` means value is **less than or equal to** x
> `..` means accept all values even if they are not comparable.

### Match on arbitrary values:

```
which (could_be_anything)
    | 1000.. => "Woah..."
    | "Hey, Sarah" => "Hi, Alice!"
    | 42 => "Oh, that's my lucky number!"
    | true => "I agree."
    | .. => "I don't know what " & could_be_anything & " is."
```

> [!NOTE]
> If there is no `..` clause in a `which` case, an error is returned when
> the value doesn't match anything specified.