# blorb

Blorb is a **formula language** that aims to be (in order):

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

Because Blorb is a formula language, it only compiles to **one value**.
It is designed to be embedded...

- as a query language?
- as part of a config language?

## Differences from...

### ... Airtable

You should be able to use your Airtable formulas with Blorb without much hassle.

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
- function, though these **can't be returned from formulae**

### Add 5 to x:

```
x + 5  # Adds 5 to x. Note that comments continue until the end of the line.
```

### Map over an array:

```
ARRAYMAP([1, 2, 3, 4], |x| x * 5)
```

### Match on some numbers:

```
which score
    1000.. => "Well done!",
    100..1000 => "Great!",
    0!..100 => "Okay!",
    ..=0 => "Oh dear",
```

> [!NOTE]
> `x..` means value is **greater than or equal to** x
> `x!..` means value is **greater than** x
> `..x` means value is **less than** x
> `..=x` means value is **less than or equal to** x
> `..` means accept all values even if they are not comparable.

### Match on arbitrary values:

```
when could_be_anything
    1000.. => "Woah...",
    "Hey, Sarah" => "Hi, Alice!",
    42 => "Oh, that's my lucky number!",
    true => "I agree.",
    .. => "I don't know what " & could_be_anything & " is."
```

> [!NOTE]
> If there is no `..` clause in a `which` case, an error is returned when
> the value doesn't match anything specified.

## Examples

### ELO

Imagine two people, Neko and Elliot. They play 5 games. Neko:

- wins the first,
- loses the second,
- loses the third,
- draws the fourth,
- wins the fifth.

Using the formulas from 
[Elo rating system](https://en.wikipedia.org/wiki/Elo_rating_system),
let's re-calculate Neko's rating after this session.

```
let
    {Neko's rating} = 1520,
    {Elliot's rating} = 1867,
    {K-factor} = 32,
    
    # Functions are values in Blorb.
    {Expected score function} = |{Player's rating}, {Opponent's rating}|
        1 / (1 + 10 ^ (({Opponent's rating} - {Player's rating}) / 400)),
    
    # Let bindings can reference previous let bindings inside of itself.
    {Neko's expected score} = {Expected score function}(
        {Neko's rating},
        {Elliot's rating}
    ),
    
    {Neko's actual scores} = [1, 0, 0, 0.5, 1],
    {Neko's total actual score} = SUM({Neko's actual scores}),
    {Neko's total expected score} = {Neko's expected score} * LEN({Neko's actual scores}),
    
    {Neko's new rating} = FLOOR(
        {Neko's rating} + (
            {K-factor} * (
                {Neko's total actual score} - {Neko's total expected score}
            )
        )
    )
of
    # When expressions allow a more ergonomic alternative to nested IF
    # calls.
    when {Neko's new rating}
        {Neko's rating} => "Neko's rating hasn't changed",
        {Neko's rating}!.. => "Neko's rating increased to " & {Neko's new rating},
        ..{Neko's rating} => "Neko's rating decreased to " & {Neko's new rating}
```

From this, we have learned:

- That functions are first-class citizens in Blorb, and can be passed as values
  and stored in variables
- That let bindings have access to variables declared **before themselves**
- That match expressions can contain expressions, not just number literals

## Security

Please report security-related bugs to **`security.lt.srh.dog`**, following
the [RFC 1035](https://datatracker.ietf.org/doc/html/rfc1035#section-8)
standard for mapping domain names to email addresses.

For all other bugs, you can use the GitHub issue tracker.

## Licensing

Blorb is currently licensed under the same license as Landtable.
This may change in the future when Blorb is complete.

For the avoidance of doubt, you may not:

- Use Blorb in any way to provide a Landtable-like service.
- Fork Blorb for the purposes of providing competing software to Blorb.
  This includes transpiling Blorb to a different language.