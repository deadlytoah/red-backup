#![allow(unused_doc_comment)]

use redundancy::Version;

error_chain! {
    errors {
        VersionMismatch(expected: Version, got: Version) {
            description("version mismatch")
            display("version mismatch (expected: {:?}, got: {:?})", expected, got)
        }
        VerificationFailure {}
        UnexpectedEndOfBlock(expected_bound: usize, got: usize) {
            description("unexpected end of block")
            display("unexpected end of block (expected: <= {}, got: {})", expected_bound, got)
        }
    }

    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error);
        Nul(::std::ffi::NulError);
        ParseInt(::std::num::ParseIntError);
        SerdeJson(::serde_json::Error);
        Utf8(::std::str::Utf8Error);
    }
}
