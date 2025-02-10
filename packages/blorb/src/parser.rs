use chumsky::{error::Simple, prelude::*, text::{self, TextParser}, Parser};

#[derive(Debug, PartialEq)]
pub enum Pattern {
    Gt(Box<Expression>),
    Geq(Box<Expression>),
    Lt(Box<Expression>),
    Leq(Box<Expression>),
    Eq(Box<Expression>),
    Else
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Num(f64),
    Str(String),
    
    Neg(Box<Expression>),
    Add(Box<Expression>, Box<Expression>),
    Sub(Box<Expression>, Box<Expression>),
    Mul(Box<Expression>, Box<Expression>),
    Div(Box<Expression>, Box<Expression>),
    FlDiv(Box<Expression>, Box<Expression>),
    Mod(Box<Expression>, Box<Expression>),
    Pow(Box<Expression>, Box<Expression>),
    Concat(Box<Expression>, Box<Expression>),
    
    Gt(Box<Expression>, Box<Expression>),
    Geq(Box<Expression>, Box<Expression>),
    Lt(Box<Expression>, Box<Expression>),
    Leq(Box<Expression>, Box<Expression>),
    Eq(Box<Expression>, Box<Expression>),
    Neq(Box<Expression>, Box<Expression>),
    
    Or(Box<Expression>, Box<Expression>),
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    
    Let {
        bindings: Vec<(String, Expression)>,
        of: Box<Expression>
    },
    
    Which {
        operand: Box<Expression>,
        ops: Vec<(Pattern, Box<Expression>)>
    },
    
    Lambda {
        args: Vec<String>,
        body: Box<Expression>
    },
    
    Call(Box<Expression>, Vec<Expression>),
    List(Vec<Expression>),
    Variable(String)
}

pub fn parser() -> impl Parser<char, Expression, Error = Simple<char>> {
    let int = text::int(10)
        .map(|s: String| Expression::Num(s.parse().unwrap()));
    
    let op = |c| just(c).padded();
    
    let escape = just('\\')
        .ignore_then(choice((
            just('\\'),
            just('/'),
            just('"'),
            just('n').to('\n'),
            just('r').to('\r'),
            just('t').to('\t'),
            just('u').ignore_then(
                text::digits(16)
                    .repeated()
                    .collect()
                    .validate(
                        |digits: String, span, emit| {
                            char::from_u32(u32::from_str_radix(&*digits, 16).unwrap())
                                .unwrap_or_else(
                                    || {
                                        emit(Simple::custom(
                                            span,
                                            "invalid unicode character"
                                        ));
                                        '\u{FFFD}' // unicode replacement character
                                    }
                                )
                        }
                    )
                    .delimited_by(just('{'), just('}'))
                )
            ))
        );
    
    let string = none_of("\\\"")
        .or(escape)
        .repeated()
        .delimited_by(just('"'), just('"'))
        .map(|x| Expression::Str(x.iter().collect()))
        .boxed();
    
    recursive(|expr| {
        let array = expr.clone()
            .separated_by(op(','))
            .collect()
            .delimited_by(just('['), just(']'))
            .map(|li| Expression::List(li));
        
        let variable = text::ident()
            .padded()
            .map(|ident| Expression::Variable(ident));
        
        let braced_variable = none_of("{}")
            .repeated()
            .delimited_by(just('{'), just('}'))
            .map(|x| Expression::Variable(x.iter().collect()));
        
        let lambda = text::ident()
            .separated_by(op(','))
            .collect()
            .delimited_by(just('|'), just('|'))
            .then(expr.clone())
            .map(|(args, body)| Expression::Lambda { args, body: Box::new(body) });
        
        let atom = variable
            .or(braced_variable)
            .or(lambda)
            .or(array)
            .or(int)
            .or(string)
            .or(expr.clone().delimited_by(just('('), just(')')));
        
        let opfold = |lhs, (op, rhs): (fn(Box<Expression>, Box<Expression>) -> Expression, _)|
            op(Box::new(lhs), Box::new(rhs));
        
        let fcall = atom.clone()
            .then(
                expr.clone()
                    .separated_by(op(','))
                    .collect()
                    .delimited_by(just('('), just(')'))
                    .repeated()
            )
            .padded()
            .foldl(|lhs, rhs| Expression::Call(Box::new(lhs), rhs));
        
        let unop = op('-').to(Expression::Neg as fn(_) -> _)
            .or(op('!').to(Expression::Not as fn(_) -> _))
            .repeated()
            .then(fcall)
            .foldr(|op, rhs| op(Box::new(rhs)));
        
        // In order of highest to lowest precedence:
        let pow = unop.clone()
            .then(
                op('^')
                .then(unop)
                .repeated()
            )
            .foldl(|lhs, (_, rhs)| Expression::Pow(Box::new(lhs), Box::new(rhs)));
        
        let muldiv = pow.clone()
            .then(
                choice((
                    op('*').to(Expression::Mul as fn(_, _) -> _),
                    op('/').to(Expression::Div as fn(_, _) -> _),
                    just("//").padded().to(Expression::FlDiv as fn(_, _) -> _),
                    op('%').to(Expression::Mod as fn(_, _) -> _)
                ))
                .then(pow)
                .repeated()
            )
            .foldl(opfold);
        
        let addsub = muldiv.clone()
            .then(
                choice((
                    op('+').to(Expression::Add as fn(_, _) -> _),
                    op('-').to(Expression::Sub as fn(_, _) -> _)
                ))
                .then(muldiv)
                .repeated()
            )
            .foldl(opfold);
        
        let concat = addsub.clone()
            .then(
                op('&')
                .then(addsub)
                .repeated()
            )
            .foldl(|lhs, (_, rhs)| Expression::Concat(Box::new(lhs), Box::new(rhs)));
        
        let cmp = concat.clone()
            .then(
                choice((
                    op('=').to(Expression::Eq as fn(_, _) -> _),
                    just('<').then_ignore(just('=').not().rewind()).padded().to(Expression::Lt as fn(_, _) -> _),
                    just('>').then_ignore(just('=').not().rewind()).padded().to(Expression::Gt as fn(_, _) -> _),
                    just("!=").padded().to(Expression::Neq as fn(_, _) -> _),
                    just(">=").padded().to(Expression::Geq as fn(_, _) -> _),
                    just("<=").padded().to(Expression::Leq as fn(_, _) -> _),
                ))
                .then(concat)
                .repeated()
            )
            .foldl(opfold);
        
        cmp
    })
        .then_ignore(end())
}