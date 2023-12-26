use std::iter::FromIterator;
use std::mem;

const DEFAULT_WIDTH: u32 = 65;
const DEFAULT_HYPHENATION: bool = false;
const SPACES: &[char] = &[' ', '\t'];

pub struct AutoFill {
    buf: String,

    width: u32,
    hyphenation: bool,

    line: String,
    overflow_word: Option<String>,
}

impl AutoFill {
    pub fn new(s: &str) -> Self {
        AutoFill {
            buf: s.into(),
            width: DEFAULT_WIDTH,
            hyphenation: DEFAULT_HYPHENATION,
            line: Default::default(),
            overflow_word: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_hyphenation(mut self, hyphen: bool) -> Self {
        self.hyphenation = hyphen;
        self
    }

    #[allow(dead_code)]
    pub fn with_width(mut self, width: u32) -> Self {
        self.width = width;
        self
    }

    fn spaces(&mut self) -> Option<String> {
        if self.buf.is_empty() {
            None
        } else {
            let mut spaces = String::default();
            for ch in self.buf.chars() {
                if SPACES.contains(&ch) {
                    spaces.push(ch);
                } else {
                    break;
                }
            }
            self.buf = self.buf.split_off(spaces.len());
            Some(spaces)
        }
    }

    fn non_spaces(&mut self) -> Option<String> {
        if self.buf.is_empty() {
            None
        } else {
            let mut word = String::default();
            for ch in self.buf.chars() {
                if !SPACES.contains(&ch) {
                    word.push(ch);
                } else {
                    break;
                }
            }
            self.buf = self.buf.split_off(word.len());
            Some(word)
        }
    }
}

impl Iterator for AutoFill {
    type Item = String;

    fn next(&mut self) -> Option<String> {
        if self.overflow_word.is_some() {
            self.line = mem::replace(&mut self.overflow_word, None).unwrap();
        }

        loop {
            let spaces = self.spaces();
            let word = self.non_spaces();

            if word.is_none() {
                // We truncate any trailing spaces.
                break;
            } else {
                let spaces = spaces.unwrap();
                let word = word.unwrap();
                let line_len = self.line.chars().count();
                let new_line_len = line_len + spaces.chars().count() + word.chars().count();

                if new_line_len > self.width as usize {
                    let directive = if self.hyphenation {
                        unimplemented!()
                    } else {
                        let overflow = DefaultOverflow::new(&spaces, &word, line_len, self.width);
                        overflow.overflow()
                    };

                    self.line.push_str(&directive.append);
                    self.overflow_word = Some(directive.next);
                    break;
                } else {
                    self.line.push_str(&spaces);
                    self.line.push_str(&word);
                }
            }
        }

        if self.line.is_empty() {
            None
        } else {
            Some(mem::replace(&mut self.line, String::default()))
        }
    }
}

struct OverflowDirective {
    append: String,
    next: String,
}

trait Overflow {
    fn overflow(self) -> OverflowDirective;
}

struct DefaultOverflow {
    spaces: String,
    word: String,
    line_len: usize,
    width: u32,
}

impl DefaultOverflow {
    fn new(spaces: &str, word: &str, line_len: usize, width: u32) -> Self {
        Self {
            spaces: spaces.into(),
            word: word.into(),
            line_len,
            width: width,
        }
    }
}

impl Overflow for DefaultOverflow {
    fn overflow(mut self) -> OverflowDirective {
        debug_assert!(self.width as usize >= self.line_len);
        let remaining = self.width as usize - self.line_len;
        let word_len = self.word.chars().count();

        if word_len > self.width as usize {
            // This is a case of a word that is too big to fit in the given
            // line width, but hyphenation is turned off.
            self.spaces.push_str(&self.word);
            let append = String::from_iter(self.spaces.chars().take(remaining).into_iter());
            let next = self.spaces.split_off(append.len());
            OverflowDirective { append, next }
        } else {
            // Overflow the word to the beginning of the next line.
            OverflowDirective {
                append: String::default(),
                next: mem::replace(&mut self.word, String::default()),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use autofill::AutoFill;

    #[test]
    fn test_auto_fill() {
        let mut af = AutoFill::new(concat!(
            "Web browsers are ubiqitous these days, ",
            "supporting wide range of platforms ",
            "including mobile and even text editor."
        ));

        assert_eq!(
            af.next().unwrap(),
            "Web browsers are ubiqitous these days, supporting wide range of"
        );
        assert_eq!(
            af.next().unwrap(),
            "platforms including mobile and even text editor."
        );
    }

    #[test]
    fn test_leading_spaces() {
        let mut af = AutoFill::new(concat!(
            "  Web browsers are ubiqitous these days, ",
            "supporting wide range of platforms ",
            "including mobile and even text editor."
        ));

        assert_eq!(
            af.next().unwrap(),
            "  Web browsers are ubiqitous these days, supporting wide range of"
        );
        assert_eq!(
            af.next().unwrap(),
            "platforms including mobile and even text editor."
        );
    }

    #[test]
    fn test_double_spaces() {
        let mut af = AutoFill::new(concat!(
            "Web browsers are ubiqitous these days.  ",
            "They support wide range of platforms ",
            "including mobile and even text editor."
        ));

        assert_eq!(
            af.next().unwrap(),
            "Web browsers are ubiqitous these days.  They support wide range"
        );
        assert_eq!(
            af.next().unwrap(),
            "of platforms including mobile and even text editor."
        );
    }

    #[test]
    fn test_long_word_without_hyphen() {
        let mut af = AutoFill::new(concat!(
            "Web browsers are ubiqitous these days,",
            "supportingwiderangeofplatforms",
            "includingmobileandeventexteditor."
        ));

        assert_eq!(
            af.next().unwrap(),
            "Web browsers are ubiqitous these days,supportingwiderangeofplatfo"
        );
        assert_eq!(af.next().unwrap(), "rmsincludingmobileandeventexteditor.");
    }
}
