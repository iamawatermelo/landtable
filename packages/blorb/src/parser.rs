use chumsky::{error::Simple, prelude::*, text::{self, TextParser}, Parser};

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
    
    Call(String, Vec<Expression>),
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
        let fcall = text::ident()
            .then(
                expr.clone()
                    .separated_by(op(','))
                    .collect()
                    .delimited_by(just('('), just(')'))
            )
            .map(|(ident, li)| Expression::Call(ident, li));
        
        let array = expr.clone()
            .separated_by(op(','))
            .collect()
            .delimited_by(just('['), just(']'))
            .map(|li| Expression::List(li));
        
        let variable = text::ident()
            .then_ignore(just('(').not())
            .padded()
            .map(|ident| Expression::Variable(ident));
        
        let braced_variable = none_of("{}")
            .repeated()
            .delimited_by(just('{'), just('}'))
            .map(|x| Expression::Variable(x.iter().collect()));
        
        let atom = variable
            .or(braced_variable)
            .or(array)
            .or(int)
            .or(string)
            .or(fcall)
            .or(expr.clone().delimited_by(just('('), just(')')));
        
        let opfold = |lhs, (op, rhs): (fn(Box<Expression>, Box<Expression>) -> Expression, _)|
            op(Box::new(lhs), Box::new(rhs));
        
        let unop = op('-').to(Expression::Neg as fn(_) -> _)
            .or(op('!').to(Expression::Not as fn(_) -> _))
            .repeated()
            .then(atom)
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
                    op('/').to(Expression::Div as fn(_, _) -> _)
                ))
                .then(pow)
                .repeated()
            )
            .foldl(opfold);
        
        let addsub = pow.clone()
            .then(
                choice((
                    op('*').to(Expression::Mul as fn(_, _) -> _),
                    op('/').to(Expression::Div as fn(_, _) -> _)
                ))
                .then(pow)
                .repeated()
            )
            .foldl(opfold);
        
        todo!()
    })
        .then_ignore(end())
}