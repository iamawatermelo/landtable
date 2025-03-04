mod parser;
mod hir;
mod bytecode;
// mod tir;

use pyo3::pymodule;

#[pymodule(module = "blorb._blorb")]
mod _blorb {
    use pyo3::{prelude::*, IntoPyObjectExt};
    use std::sync::Arc;
    use std::collections::BTreeMap;
    use chumsky::error::Simple;
    use std::ops::Range;

    use crate::{hir::{Hir, HirError}, parser::{compute_line_map, parse, ParsedFormula}};
    
    #[pyclass]
    struct WrappedFormula {
        inner: Hir
    }
    
    #[pymethods]
    impl WrappedFormula {
        fn __str__(&self) -> String {
            format!("{:#?}", self.inner)
        }
    }
    
    #[pyclass]
    #[derive(Debug, Clone)]
    struct WrappedCompilationError {
        source_length: usize,
        map: Arc<BTreeMap<usize, usize>>,
        span: Range<usize>,
        
        #[pyo3(get)]
        message: String,
        
        #[pyo3(get)]
        associated_hints: Vec<WrappedCompilationError>
    }
    
    #[pyclass(get_all)]
    #[derive(Debug, Clone)]
    struct UserSpan {
        line: usize,
        column: usize,
        line_index_start: usize,
        line_index_end: usize
    }
    
    #[pymethods]
    impl WrappedCompilationError {
        #[getter]
        fn span(&self) -> (usize, usize) {
            return (self.span.start, self.span.end)
        }
        
        #[getter]
        /// Tuple of (line, column, line start index, line end index)
        fn line_col(&self) -> Option<UserSpan> {
            let (offset, line) = self.map.range(..=self.span.start).last()?;
            let line_index_end = self.map.range(self.span.start+1..).next()
                .map(|(o, _)| *o - 1)
                .unwrap_or(self.source_length);
            
            let column = (self.span.start - offset) + 1;
            
            Some(UserSpan {
                line: *line,
                column,
                line_index_start: *offset,
                line_index_end
            })
        }
        
        fn __str__(&self) -> String {
            let Some((offset, line)) = self.map.range(..=self.span.start).last() else {
                return format!("error at ?:? (span {} - {}): {}", self.span.start, self.span.end, self.message)
            };
            
            let col = (self.span.start - offset) + 1;
            
            format!("error at {line}:{col}: {}", self.message)
        }
        
        fn __repr__(&self) -> String {
            format!("WrappedCompilationError(({}, {}), {})", self.span.start, self.span.end, self.message)
        }
    }
    
    #[pyfunction]
    fn compile(py: Python, src: String) -> PyResult<PyObject> {
        let ast = match parse(src.clone().into_boxed_str()) {
            Ok(ast) => ast,
            Err(errors) => return {
                let map = Arc::new(compute_line_map(&src));
                
                errors.into_iter().map(|e: Simple<_>|
                    WrappedCompilationError {
                        source_length: src.len(),
                        map: map.clone(),
                        span: e.span(),
                        message: format!("{}", e),
                        associated_hints: Vec::new()
                    }
                ).collect::<Vec<WrappedCompilationError>>().into_py_any(py)
            }
        };
        
        match Hir::lower(ast.ast) {
            Ok(hir) => WrappedFormula {
                inner: hir
            }.into_py_any(py),
            Err(errors) => {
                let map = Arc::new(compute_line_map(&src));
                
                errors.into_iter().map(|e: HirError| match e {
                    HirError::VariableNotFound(x) => WrappedCompilationError {
                        source_length: src.len(),
                        map: map.clone(),
                        span: x.span,
                        message: format!("unknown variable {}", x.inner),
                        associated_hints: Vec::new()
                    },
                    HirError::VariableRedeclared(initial, new) => WrappedCompilationError {
                        source_length: src.len(),
                        map: map.clone(),
                        span: new.span,
                        message: format!("not allowed to declare {} twice in the same let binding", new.inner),
                        associated_hints: vec![
                            WrappedCompilationError {
                                source_length: src.len(),
                                map: map.clone(),
                                span: initial.span,
                                message: format!("initial declaration of {}", initial.inner),
                                associated_hints: Vec::new()
                            }
                        ]
                    },
                    HirError::NotImplemented(x) => WrappedCompilationError {
                        source_length: src.len(),
                        map: map.clone(),
                        span: x.span,
                        message: format!("internal error: not implemented (HIR): {:#?}", x.inner),
                        associated_hints: Vec::new()
                    }
                }).collect::<Vec<WrappedCompilationError>>().into_py_any(py)
            }
        }
    }
}