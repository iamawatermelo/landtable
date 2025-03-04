/// The HIR, with additional type information. This is used to perform
/// certain optimisations.
use std::collections::HashSet;

#[derive(Clone, Hash)]
pub struct TypeUnion {
    members: HashSet<ConcreteType>
}

impl From<ConcreteType> for TypeUnion {
    fn from(value: ConcreteType) -> Self {
        let mut members = HashSet::new();
        members.insert(value);
        
        Self {
            members
        }
    }
}

#[derive(Clone, Hash)]
pub enum ConcreteType {
    Number,
    String,
    Boolean,
    Empty,
    Error,
    Function {
        args: Vec<TypeUnion>,
        ret: TypeUnion
    }
}

#[derive(Debug, PartialEq)]
pub enum TirOp {
    Num(f64),
    Str(Box<str>),
    Variable {
        id: VarId,
        scope: ScopeId,
        
        /// Whether this variable is a user-defined variable or one
        /// inserted by the compiler.
        real: bool
    },
    
    // Normal operations
    Neg(Op),
    Add(Op, Op),
    Sub(Op, Op),
    Mul(Op, Op),
    Div(Op, Op),
    FlDiv(Op, Op),
    Mod(Op, Op),
    Pow(Op, Op),
    Concat(Op, Op),
    
    Gt(Op, Op),
    Geq(Op, Op),
    Lt(Op, Op),
    Leq(Op, Op),
    Eq(Op, Op),
    Neq(Op, Op),
    
    Or(Op, Op),
    And(Op, Op),
    Not(Op),
    
    // HIR constructs
    /// If .0 then .1 else .2
    If(Op, Op, Op),
    
    Scope(Arc<HirScope>, Op),
    
    /// For the value Error, not an invalid HIR node (see Invalid).
    ErrorValue,
    
    Invalid
}