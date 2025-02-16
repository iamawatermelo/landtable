mod parser;

use pyo3::pymodule;

#[pymodule(module = "blorb._blorb")]
mod _blorb {
    use pyo3::{prelude::*, IntoPyObjectExt};
    use chumsky::{error::Simple, Parser};
    use std::ops::Range;

    use crate::parser::{Expression, parse};
    
    #[pyclass]
    struct WrappedAST {
        inner: Expression
    }
    
    #[pymethods]
    impl WrappedAST {
        fn __str__(&self) -> String {
            format!("{:#?}", self.inner)
        }
    }
    
    #[pyclass]
    struct WrappedCompilationError {
        span: Range<usize>,
        
        #[pyo3(get)]
        message: String
    }
    
    #[pymethods]
    impl WrappedCompilationError {
        #[getter]
        fn span(&self) -> (usize, usize) {
            return (self.span.start, self.span.end)
        }
        
        fn __str__(&self) -> String {
            format!("error at {}:{}: {}", self.span.start, self.span.end, self.message)
        }
        
        fn __repr__(&self) -> String {
            format!("WrappedCompilationError(({}, {}), {})", self.span.start, self.span.end, self.message)
        }
    }
    
    #[pyfunction]
    fn compile(py: Python, src: String) -> PyResult<PyObject> {
        match parse(src) {
            Ok(ast) => WrappedAST { inner: ast.inner }.into_py_any(py),
            Err(errors) => errors.into_iter().map(|e: Simple<_>| WrappedCompilationError {
                span: e.span(),
                message: format!("{}", e)
            }).collect::<Vec<WrappedCompilationError>>().into_py_any(py)
        }
    }
}