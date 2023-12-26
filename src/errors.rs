// Create the Error, ErrorKind, ResultExt, and Result types
error_chain!{
    errors {
        EmptyUnitSet
    }

    foreign_links {
        VerifileError(::verifile::Error);
    }
}
