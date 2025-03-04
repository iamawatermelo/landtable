/// Blorb's HIR.
/// The HIR contains resolved variable information, as well as
/// desugaring of some language constructs.

use std::collections::{HashMap, HashSet};
use bitflags::bitflags;
use thiserror::Error;
use crate::parser::{Expression, Tagged};

type Op = Box<Tagged<HirOp>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopeId(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarId(u32);

bitflags! {
    /// Represents the origin of a scope.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct ScopeFlags: u32 {
        /// This scope was created from a let binding.
        /// Variables in this scope will have a name binding.
        const FROM_LET_BINDING = 0b00000001;
        
        /// This scope was implicitly created from a when binding.
        /// This scope will only have one variable inside it.
        const FROM_WHEN_STATEMENT = 0b00000010;
    }
}

#[derive(Debug, PartialEq)]
pub struct HirScope {
    flags: ScopeFlags,
    variables: Vec<(VarId, Op)>,
    name_to_variable_map: HashMap<Box<str>, Tagged<VarId>>,
    inner: Op
}

#[derive(Debug, PartialEq)]
pub enum HirOp {
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
    
    Scope(ScopeId),
    
    /// For the value Error, not an invalid HIR node (see Invalid).
    ErrorValue,
    
    Invalid
}

#[derive(Debug, Error)]
pub enum HirError {
    #[error("unknown variable")]
    VariableNotFound(Tagged<Box<str>>),
    
    #[error("variable redeclared")]
    /// When a variable has been declared twice in the same let block.
    /// first: initial declaration, second: this declaration
    VariableRedeclared(Tagged<Box<str>>, Tagged<Box<str>>),
    
    #[error("not implemented")]
    NotImplemented(Tagged<Expression>),
    
    #[error("internal error")]
    HirInternalError(Box<str>)
}

#[derive(Debug)]
pub struct HirResolutionContext {
    pub(crate) interned_strings: HashSet<Box<str>>,
    pub(crate) variables: HashMap<VarId, Tagged<Box<str>>>,
    pub(crate) scopes: HashMap<ScopeId, HirScope>,
    pub(crate) errors: Vec<HirError>,
    scope_stack: Vec<ScopeId>,
    pub(crate) scope_gen: u32,
    pub(crate) var_gen: u32
}

#[derive(Debug)]
pub struct Hir {
    pub(crate) root: Op,
    pub(crate) ctx: HirResolutionContext
}

impl HirResolutionContext {
    fn new() -> HirResolutionContext {
        HirResolutionContext {
            interned_strings: HashSet::new(),
            variables: HashMap::new(),
            scopes: HashMap::new(),
            errors: Vec::new(),
            scope_stack: Vec::new(),
            scope_gen: 1,
            var_gen: 0
        }
    }
    
    /// Create a new scope. Once the scope is created, it can't be
    /// modified.
    fn scope<F: FnOnce(&mut Self, ScopeId) -> HirScope>(&mut self, f: F) -> ScopeId {
        let x = ScopeId(self.scope_gen);
        self.scope_gen += 1;
        self.scopes.insert(x, f(self, x));
        
        x
    }
    
    /// Enter a scope.
    fn enter_scope<F: FnOnce(&mut Self) -> Op>(&mut self, scope: ScopeId, f: F) -> Op {
        self.scope_stack.push(scope.clone());
        let ret = f(self);
        self.scope_stack.pop();
        
        ret
    }
    
    /// Resolve a variable name in the current context.
    /// Returns a tuple of scope ID and variable ID.
    fn resolve_variable_name(&mut self, name: &Box<str>) -> Option<(ScopeId, VarId)> {
        for scope_id in self.scope_stack.iter().rev() {
            let Some(scope) = self.scopes.get(scope_id) else {
                self.emit_error(HirError::HirInternalError(format!("scope {scope_id:#?} was in the scope stack but it does not exist").into()));
                
                return None
            };
            
            if let Some(id) = scope.name_to_variable_map.get(name) {
                return Some(((*scope_id, id.inner)))
            }
        }
        
        None
    }
    
    /// Create a new variable and allocate a variable ID for it.
    fn variable(&mut self, name: Tagged<Box<str>>) -> VarId {
        let x = VarId(self.var_gen);
        let interned_name = name.map(|x| self.intern_string(x));
        self.var_gen += 1;
        assert!(self.variables.insert(x, interned_name).is_none(), "sanity check: var id {x:#?} already exists");
        
        x
    }
    
    /// Emit an error while lowering.
    fn emit_error(&mut self, err: HirError) {
        self.errors.push(err);
    }
    
    /// Intern a string.
    fn intern_string(&mut self, to_intern: Box<str>) -> Box<str> {
        if let Some(interned_string) = self.interned_strings.get(&to_intern) {
            return interned_string.clone()
        }
        
        self.interned_strings.insert(to_intern.clone());
        
        to_intern
    }
}

impl Hir {
    pub fn lower(ast: Tagged<Expression>) -> Result<Self, Vec<HirError>> {
        let mut ctx = HirResolutionContext::new();
        let root = Hir::_lower(ast, &mut ctx);
        
        assert!(ctx.scope_stack.len() == 0, "sanity check: scope stack len != 0");
        
        if ctx.errors.len() != 0 {
            return Err(ctx.errors)
        }
        
        Ok(Hir {
            root,
            ctx
        })
    }
    
    fn _lower(ast: Tagged<Expression>, ctx: &mut HirResolutionContext) -> Op {
        let span = ast.span.start..ast.span.end;
        
        Box::new(ast.map(|expr| match expr {
            Expression::Num(x) => HirOp::Num(x),
            Expression::Str(x) => HirOp::Str(ctx.intern_string(x)),
            
            // Truly, who needs macro_rules! when you have regexr
            Expression::Add(lhs, rhs) => HirOp::Add(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Sub(lhs, rhs) => HirOp::Sub(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Mul(lhs, rhs) => HirOp::Mul(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Div(lhs, rhs) => HirOp::Div(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::FlDiv(lhs, rhs) => HirOp::FlDiv(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Mod(lhs, rhs) => HirOp::Mod(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Pow(lhs, rhs) => HirOp::Pow(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Concat(lhs, rhs) => HirOp::Concat(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            
            Expression::Gt(lhs, rhs) => HirOp::Gt(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Geq(lhs, rhs) => HirOp::Geq(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Lt(lhs, rhs) => HirOp::Lt(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Leq(lhs, rhs) => HirOp::Leq(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Eq(lhs, rhs) => HirOp::Eq(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::Neq(lhs, rhs) => HirOp::Neq(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            
            Expression::Or(lhs, rhs) => HirOp::Or(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            Expression::And(lhs, rhs) => HirOp::And(Hir::_lower(*lhs, ctx), Hir::_lower(*rhs, ctx)),
            
            // Unary ops
            Expression::Not(op) => HirOp::Not(Hir::_lower(*op, ctx)),
            Expression::Neg(op) => HirOp::Neg(Hir::_lower(*op, ctx)),
            
            // Variables
            Expression::Variable(name) => {
                let Some((scope, id)) = ctx.resolve_variable_name(&name) else {
                    ctx.emit_error(HirError::VariableNotFound(Tagged::new(span, name)));
                    
                    return HirOp::Invalid
                };
                
                HirOp::Variable { id, scope, real: true }
            }
            
            // Lower let bindings
            Expression::Let { bindings, of } => {
                let mut name_to_variable_map = HashMap::new();
                let mut variables = Vec::new();
                
                for (ident, expr) in bindings {
                    let interned_ident = ident.clone().map(|x| ctx.intern_string(x));
                    let id = ident.map(|_| ctx.variable(interned_ident.clone()));
                    if let Some(x) = name_to_variable_map.insert(interned_ident.inner.clone(), id.clone()) {
                        ctx.emit_error(HirError::VariableRedeclared(
                            x.map(|_| interned_ident.inner.clone()),
                            interned_ident
                        ));
                    }
                    
                    variables.push((id.inner, Hir::_lower(expr, ctx)));
                }
                
                let scope = ctx.scope(|ctx, id| HirScope {
                    name_to_variable_map,
                    variables,
                    flags: ScopeFlags::FROM_LET_BINDING,
                    inner: ctx.enter_scope(id, |ctx| Hir::_lower(*of, ctx))
                });
                
                HirOp::Scope(scope)
            }
            
            _ => {
                ctx.emit_error(HirError::NotImplemented(Tagged::new(span, expr)));
                
                HirOp::Invalid
            }
        }))
    }
}