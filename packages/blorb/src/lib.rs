mod parser;

use chumsky::Parser;
use parser::parser;
use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn test(a: String) -> PyResult<()> {
    let (ast, errors) = parser().parse_recovery_verbose(a);
    
    if errors.len() == 0 {
        println!("parsing raised 0 errors");
        println!("{ast:#?}")
    } else {
        println!("parsing raised {} errors", errors.len());
        println!("{errors:#?}");
        println!("recovered AST: {ast:#?}")
    }
    
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn blorb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(test, m)?)?;
    Ok(())
}
