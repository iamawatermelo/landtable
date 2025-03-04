use std::{alloc::GlobalAlloc, ops::Range};
use std::iter::once;
use std::collections::BTreeMap;
use chumsky::{error::Simple, prelude::*, primitive::custom, text::{self, TextParser}, Parser, Stream};
use logos::{Logos, Span};

#[derive(Logos, Debug, PartialEq, Clone, Hash, Eq)]
#[logos(skip r"[ \t\n\f]+")]
#[logos(skip r"#.*\n?")]
pub enum Token {
    #[token("let")]
    Let,
    
    #[token("of")]
    Of,
    
    #[token(":=")]
    Assign,
    
    #[token("when")]
    When,
    
    #[token("=>")]
    Arm,
    
    #[token("[")]
    LeftList,
    
    #[token("]")]
    RightList,
    
    #[token("(")]
    LeftParen,
    
    #[token(")")]
    RightParen,
    
    #[token(",")]
    Separator,
    
    #[regex(r#""([^\\"]|\\[\w"\\])*""#)]
    String,
    
    #[regex(r"[0-9_]+(\.[0-9_]+)?", priority = 3)]
    Number,
    
    #[regex(r"\{[^}]+\}")]
    BracketedVariable,
    
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*")]
    Variable,
    
    // Math operations
    #[token("-")]
    Minus,
    
    #[token("+")]
    Plus,
    
    #[token("*")]
    Multiply,
    
    #[token("/")]
    Divide,
    
    #[token("//")]
    FloorDivide,
    
    #[token("&")]
    Concatenate,
    
    #[token("%")]
    Modulo,
    
    #[token("^")]
    Power,
    
    // Logic operators
    #[token("=")]
    Eq,
    
    #[token("!=")]
    Neq,
    
    #[token("|")]
    Pipe,
    
    #[token("&&")]
    And,
    
    #[token("!")]
    Not,
    
    // Comparison operators
    #[token("<")]
    Lt,
    
    #[token("<=")]
    Leq,
    
    #[token(">")]
    Gt,
    
    #[token(">=")]
    Geq,
    
    // Pattern matchinbg
    #[token("..")]
    Bt,
    
    #[token("..=")]
    BtInc,
    
    #[token("!..")]
    BtEx,
    
    #[token("!..=")]
    BtExInc,
    
    Error(Box<str>),
    Eof
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Let => write!(f, "let"),
            Self::Of => write!(f, "of"),
            Self::Assign => write!(f, ":="),
            Self::When => write!(f, "when"),
            Self::Arm => write!(f, "match arm"),
            Self::LeftList => write!(f, "opening list bracket"),
            Self::RightList => write!(f, "closing list bracket"),
            Self::LeftParen => write!(f, "opening parenthesis"),
            Self::RightParen => write!(f, "closing parenthesis"),
            Self::Separator => write!(f, "separator"),
            Self::String => write!(f, "string literal"),
            Self::Number => write!(f, "number literal"),
            Self::Variable => write!(f, "variable"),
            Self::BracketedVariable => write!(f, "bracketed variable"),
            Self::Minus => write!(f, "-"),
            Self::Plus => write!(f, "+"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::FloorDivide => write!(f, "//"),
            Self::Concatenate => write!(f, "&"),
            Self::Modulo => write!(f, "%"),
            Self::Power => write!(f, "^"),
            Self::Eq => write!(f, "="),
            Self::Neq => write!(f, "!="),
            Self::Pipe => write!(f, "|"),
            Self::And => write!(f, "&&"),
            Self::Not => write!(f, "!"),
            Self::Lt => write!(f, "<"),
            Self::Leq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::Geq => write!(f, ">="),
            Self::Eof => write!(f, "end of formula"),
            Self::Bt => write!(f, ".."),
            Self::BtInc => write!(f, "..="),
            Self::BtEx => write!(f, "!.."),
            Self::BtExInc => write!(f, "!..="),

            Self::Error(x) => write!(f, "[unrecognised token {x}]"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Tagged<T: std::fmt::Debug + PartialEq> {
    pub span: Range<usize>,
    pub inner: T
}

impl<T: std::fmt::Debug + PartialEq> Tagged<T> {
    pub fn new(span: Range<usize>, inner: T) -> Tagged<T> {
        Tagged {
            span,
            inner
        }
    }
    
    pub fn map<N: std::fmt::Debug + PartialEq, F: FnOnce(T) -> N>(self, f: F) -> Tagged<N> {
        Tagged {
            span: self.span,
            inner: f(self.inner)
        }
    }
}

pub type Expr = Box<Tagged<Expression>>;

#[derive(Debug, PartialEq)]
pub enum Pattern {
    /// .0 <= x < .1
    Bt(Expr, Expr),
    
    /// .0 <= x <= .1
    BtInc(Expr, Expr),
    
    /// .0 < x < .1
    BtEx(Expr, Expr),
    
    /// .0 < x <= 1
    BtExInc(Expr, Expr),
    
    Lt(Expr),
    Leq(Expr),
    Gt(Expr),
    Geq(Expr),
    
    Eq(Expr),
    
    Else,
    
    Invalid
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Num(f64),
    Str(Box<str>),
    
    Neg(Expr),
    Add(Expr, Expr),
    Sub(Expr, Expr),
    Mul(Expr, Expr),
    Div(Expr, Expr),
    FlDiv(Expr, Expr),
    Mod(Expr, Expr),
    Pow(Expr, Expr),
    Concat(Expr, Expr),
    
    Gt(Expr, Expr),
    Geq(Expr, Expr),
    Lt(Expr, Expr),
    Leq(Expr, Expr),
    Eq(Expr, Expr),
    Neq(Expr, Expr),
    
    Or(Expr, Expr),
    And(Expr, Expr),
    Not(Expr),
    
    Let {
        bindings: Vec<(Tagged<Box<str>>, Tagged<Expression>)>,
        of: Expr
    },
    
    When {
        operand: Expr,
        ops: Vec<(Tagged<Pattern>, Tagged<Expression>)>
    },
    
    Lambda {
        args: Vec<Tagged<Box<str>>>,
        body: Expr
    },
    
    Call {
        func: Expr,
        args: Vec<Tagged<Expression>>
    },
    
    List(Vec<Tagged<Expression>>),
    Variable(Box<str>),
    
    Invalid
}

macro_rules! gen_parser_precedence {
    ($from:ident, {$($token:path => $expr:path),+}) => {
        $from.clone()
            .then(
                choice((
                    $( just($token).to($expr as fn(_, _) -> _), )+
                ))
                .map_with_span(|f, span| (f, span))
                .then($from)
                .repeated()
            )
            .foldl(|lhs, ((op, span), rhs)| Tagged::new(span, op(Box::new(lhs), Box::new(rhs))))
            // it takes like nine years to compile if I don't box this I'm so sorry
            .boxed()
    }
}

fn parser<'a>(source: &'a Box<str>) -> impl Parser<Token, Tagged<Expression>, Error = Simple<Token>> + use<'a> {
    let number = just(Token::Number)
        .validate(|_, span: Span, emit| {
            match source[span.start..span.end].replace('_', "").parse::<f64>() {
                Ok(n) => Tagged::new(span, Expression::Num(n)),
                Err(e) => {
                    emit(Simple::custom(span.clone(), format!("invalid number literal {e}")));
                    
                    Tagged::new(span, Expression::Invalid)
                }
            }
        });
    
    let string = just(Token::String)
        .map_with_span(|_, span: Span| Tagged::new(
            span.start..span.end, 
            Expression::Str(source[span.start+1..span.end-1].into())
        ));
    
    let variable = just(Token::Variable)
        .map_with_span(|_, span: Span| Tagged::new(span.start..span.end, Expression::Variable(source[span].into())))
        .or(
            just(Token::BracketedVariable)
                .map_with_span(|_, span: Span| Tagged::new(
                    span.start..span.end, 
                    Expression::Variable(source[span.start+1..span.end-1].into())
                ))
        );
    
    recursive(|expr| {
        let atom = recursive(|atom| {
            let pattern = atom.clone()
                .or_not()
                .then(choice((
                    just(Token::Bt),
                    just(Token::BtInc),
                    just(Token::BtEx),
                    just(Token::BtExInc),
                )))
                .then(atom.clone().or_not())
                .validate(|((lhs, op), rhs), span: Span, emit| Tagged::new(
                    span.start..span.end, 
                    match (lhs, op, rhs) {
                        // Base case
                        (None, Token::Bt, None) => Pattern::Else,
                        
                        // Ranged
                        (Some(lhs), Token::Bt, Some(rhs)) => Pattern::Bt(Box::new(lhs), Box::new(rhs)),
                        (Some(lhs), Token::BtInc, Some(rhs)) => Pattern::BtInc(Box::new(lhs), Box::new(rhs)),
                        (Some(lhs), Token::BtEx, Some(rhs)) => Pattern::BtEx(Box::new(lhs), Box::new(rhs)),
                        (Some(lhs), Token::BtExInc, Some(rhs)) => Pattern::BtExInc(Box::new(lhs), Box::new(rhs)),
                        
                        // Open-ended on lhs
                        (Some(lhs), Token::Bt, None) => Pattern::Geq(Box::new(lhs)),
                        (Some(lhs), Token::BtEx, None) => Pattern::Gt(Box::new(lhs)),
                        
                        // Open-ended on rhs
                        (None, Token::Bt, Some(rhs)) => Pattern::Lt(Box::new(rhs)),
                        (None, Token::BtInc, Some(rhs)) => Pattern::Leq(Box::new(rhs)),
                        
                        _ => {
                            emit(Simple::custom(span, format!("invalid range specifier")));
                            
                            Pattern::Invalid
                        }
                    }
                ))
                .or(
                    atom.clone()
                        .map_with_span(|thing, span| Tagged::new(span, Pattern::Eq(Box::new(thing))))
                );
            
            let when = just(Token::When)
                .then(atom.clone())
                .then(
                    pattern
                        .then_ignore(just(Token::Arm))
                        .then(expr.clone())
                        .separated_by(just(Token::Separator))
                        .collect()
                )
                .map_with_span(|((_, operand), ops), span| Tagged::new(
                    span,
                    Expression::When {
                        operand: Box::new(operand),
                        ops
                    }
                ));
            
            let binding = variable
                .clone()
                .then_ignore(just(Token::Assign))
                .then(expr.clone())
                .map(|(var, rhs)| (var.map(|inner| match inner {
                    Expression::Variable(name) => name,
                    _ => "???".into()
                }), rhs))
                .separated_by(just(Token::Separator))
                .delimited_by(just(Token::Let), just(Token::Of))
                .then(expr.clone())
                .map_with_span(|(bindings, of), span| Tagged::new(
                    span,
                    Expression::Let {
                        bindings,
                        of: Box::new(of)
                    }
                ));
            
            let list = expr.clone()
                .separated_by(just(Token::Separator))
                .collect()
                .map_with_span(|list, span: Span| Tagged::new(
                    span,
                    Expression::List(list)
                ))
                .delimited_by(just(Token::LeftList), just(Token::RightList));
            
            let paren_expr = expr.clone()
                .delimited_by(just(Token::LeftParen), just(Token::RightParen));
            
            let lambda = variable
                .clone()
                .map(|var| var.map(|inner| match inner {
                    Expression::Variable(name) => name,
                    _ => "???".into()
                }))
                .separated_by(just(Token::Separator))
                .collect()
                .delimited_by(just(Token::Pipe), just(Token::Pipe))
                .then(expr.clone())
                .map_with_span(|(args, body), span| Tagged::new(
                    span,
                    Expression::Lambda {
                        args,
                        body: Box::new(body)
                    }
                ));
                
            choice((
                number,
                string,
                variable,
                when,
                binding,
                list,
                paren_expr,
                lambda
            ))
        })
            .boxed();
        
        let fcall = atom.clone()
            .then(
                expr
                .clone()
                .separated_by(just(Token::Separator))
                .collect::<Vec<Tagged<Expression>>>()
                .delimited_by(just(Token::LeftParen), just(Token::RightParen))
                .map_with_span(|arglist, span| (arglist, span))
                .repeated()
            )
            .foldl(|func, (args, span)| Tagged::new(span, Expression::Call {
                func: Box::new(func),
                args
            }));
        
        let unary = choice((
            just(Token::Minus).to(Expression::Neg as fn(_) -> _),
            just(Token::Not).to(Expression::Not as fn(_) -> _)
        ))
            .map_with_span(|f, span| (f, span))
            .repeated()
            .then(fcall.clone())
            .foldr(|(op, span), rhs| Tagged::new(span, op(Box::new(rhs))));
        
        let power = gen_parser_precedence!(unary, {
            Token::Power => Expression::Pow
        });
        
        let muldiv = gen_parser_precedence!(power, {
            Token::Multiply => Expression::Mul,
            Token::Divide => Expression::Div,
            Token::FloorDivide => Expression::FlDiv,
            Token::Modulo => Expression::Mod
        });
        
        let addsub = gen_parser_precedence!(muldiv, {
            Token::Plus => Expression::Add,
            Token::Minus => Expression::Sub
        });
        
        let concat = gen_parser_precedence!(addsub, {
            Token::Concatenate => Expression::Concat
        });
        
        let band = gen_parser_precedence!(concat, {
            Token::And => Expression::And
        });
        
        let bor = gen_parser_precedence!(band, {
            Token::Pipe => Expression::Or
        });
        
        let cmp = gen_parser_precedence!(bor, {
            Token::Eq => Expression::Eq,
            Token::Neq => Expression::Neq,
            Token::Lt => Expression::Lt,
            Token::Leq => Expression::Leq,
            Token::Gt => Expression::Gt,
            Token::Geq => Expression::Geq
        });
        
        cmp
    })
        .then_ignore(end())
}

#[derive(Debug)]
pub struct ParsedFormula {
    pub source: Box<str>,
    pub ast: Tagged<Expression>
}

pub fn parse(source: Box<str>) -> Result<ParsedFormula, Vec<Simple<Token>>> {
    let lexer = Token::lexer(&*source);
    
    let tokens = lexer
        .spanned()
        .map(|(maybe_token, span)| match maybe_token {
            Ok(token) => (token, span),
            Err(_) => (Token::Error(source[span.start..span.end].into()), span)
        });
    
    let stream = Stream::from_iter(0..source.len(), tokens);
    
    let ast = parser(&source).parse(stream)?;
    
    Ok(ParsedFormula {
        source,
        ast
    })
}

pub fn compute_line_map(source: &str) -> BTreeMap<usize, usize> {
    source.chars()
        .enumerate()
        .filter(|(_, char)| *char == '\n')
        .enumerate()
        .map(|(lineno, (offset, _))| (offset, lineno + 2))
        .chain(once((0, 1)))
        .collect::<BTreeMap<usize, usize>>()
}