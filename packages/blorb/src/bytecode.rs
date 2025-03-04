use std::collections::HashMap;

use crate::{hir::{Hir, HirOp}, parser::Tagged};

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Hash)]
enum Value {
    Number(f64),
    String,
    Boolean,
    Empty,
    Error,
    Fn(u16)
}

#[derive(Debug, Copy, Clone)]
#[repr(u16)]
enum Operation {
    /// Push a constant to the stack.
    PushConstant,
    
    /// Push a slot entry to the stack.
    PushSlot,
    
    /// Pop a slot entry from the stack.
    PopSlot,
    
    /// -stack0
    Neg,
    
    /// stack0 + stack1
    Add,
    
    /// stack0 - stack1
    Sub,
    
    /// stack0 * stack1
    Mul,
    
    /// stack0 / stack1
    Div,
    
    /// stack0 // stack1
    FlDiv,
    
    /// stack0 % stack1
    Mod,
    
    /// stack0 ^ stack1
    Pow,
    
    /// stack0 & stack1
    Concat,
    
    /// stack0 > stack1
    Gt,
    
    /// stack0 >= stack1
    Geq,
    
    /// stack0 < stack1
    Lt,
    
    /// stack0 <= stack1
    Leq,
    
    /// stack0 = stack1
    Eq,
    
    /// stack0 != stack1
    Neq,
    
    /// stack0 | stack1
    Or,
    
    /// stack0 && stack1
    And,
    
    /// !stack0
    Not,
    
    /// Jump to the instruction specified by its operand.
    Jump
}

#[derive(Debug)]
struct Bytecode {
    /// Constants used in the bytecode
    constants: Vec<Value>,
    
    /// How many slots to allocate
    slot_length: usize,
    
    /// The actual bytecode
    code: Vec<(Operation, u16)>
}

impl Bytecode {
    fn emit_op(&mut self, op: Operation) {
        self.code.push((op, 0));
    }
    
    fn emit_op_operand(&mut self, op: Operation, operand: u16) {
        self.code.push((op, operand));
    }
    
    fn walk(&mut self, op: Box<Tagged<HirOp>>) {
        match op {
            
            
            _ => todo!()
        }
    }
    
    pub fn compile(hir: Hir) {
        let mut compiler = Bytecode {
            constants: Vec::new(),
            slot_length: hir.ctx.var_gen as usize,
            code: Vec::new()
        };
        
        compiler.walk(hir.root);
    }
}

