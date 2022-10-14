use std::{iter::Peekable, str::Chars};


struct Lexer<'a> {
    lexer: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    fn new(command: &'a str) -> Self {
        Lexer { lexer: command.chars().peekable() }
    }

    fn next_if<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<char> {
        self.lexer.peek().filter(|&&c| predicate(c))?;
        self.lexer.next()
    }

    fn next_while<F: Fn(char) -> bool>(&mut self, predicate: F) -> Option<String> {
        let mut s = String::new();
        while let Some(ch) = self.next_if(&predicate) {
            s.push(ch);
        }
        Some(s).filter(|s| !s.is_empty())
    }

    fn consume_whitespace(&mut self) -> Option<String> {
        self.next_while(|c| c.is_whitespace())
    }
}


#[cfg(test)]
mod test {
    use super::*;

    //#[test]
    fn lexer_test() {
        let temp = "hellow world haha";
        let mut lexer = Lexer::new(temp);
        let result = lexer.consume_whitespace();
        assert_eq!(result, None);
        assert_eq!(temp, "hellowworldhaha");
    }
    
}