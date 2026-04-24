package io.conduktor.kri.query

/**
 * Grammar:
 *   expr    := or
 *   or      := and ('OR' and)*
 *   and     := not ('AND' not)*
 *   not     := 'NOT' not | atom
 *   atom    := '(' expr ')' | predicate
 *   predicate := IDENT ':' VALUE (',' VALUE)*
 *   VALUE   := bare-chars | '"' quoted '"'
 *
 * Operators are case-insensitive.
 */
sealed interface FilterAst {
    data class Predicate(
        val dim: String,
        val values: List<String>,
    ) : FilterAst

    data class And(
        val left: FilterAst,
        val right: FilterAst,
    ) : FilterAst

    data class Or(
        val left: FilterAst,
        val right: FilterAst,
    ) : FilterAst

    data class Not(
        val inner: FilterAst,
    ) : FilterAst

    data object True : FilterAst
}

class FilterParseException(
    msg: String,
) : RuntimeException(msg)

class FilterParser(
    private val src: String,
) {
    private var pos = 0

    companion object {
        fun parse(src: String?): FilterAst {
            val t = src?.trim().orEmpty()
            if (t.isEmpty()) return FilterAst.True
            val p = FilterParser(t)
            val ast = p.parseOr()
            p.skipWs()
            if (p.pos != p.src.length) throw FilterParseException("trailing input at ${p.pos}: '${p.src.substring(p.pos)}'")
            return ast
        }
    }

    private fun parseOr(): FilterAst {
        var left = parseAnd()
        while (true) {
            skipWs()
            if (!matchKeyword("OR")) break
            val right = parseAnd()
            left = FilterAst.Or(left, right)
        }
        return left
    }

    private fun parseAnd(): FilterAst {
        var left = parseNot()
        while (true) {
            skipWs()
            if (!matchKeyword("AND")) break
            val right = parseNot()
            left = FilterAst.And(left, right)
        }
        return left
    }

    private fun parseNot(): FilterAst {
        skipWs()
        return if (matchKeyword("NOT")) FilterAst.Not(parseNot()) else parseAtom()
    }

    private fun parseAtom(): FilterAst {
        skipWs()
        if (peek() == '(') {
            pos++
            val inner = parseOr()
            skipWs()
            if (peek() != ')') throw FilterParseException("expected ')' at $pos")
            pos++
            return inner
        }
        return parsePredicate()
    }

    private fun parsePredicate(): FilterAst {
        skipWs()
        val dim = readIdent()
        skipWs()
        if (peek() != ':') throw FilterParseException("expected ':' after '$dim' at $pos")
        pos++
        val values = mutableListOf(readValue())
        while (true) {
            val save = pos
            skipWs()
            if (peek() == ',') {
                pos++
                values.add(readValue())
            } else {
                pos = save
                break
            }
        }
        return FilterAst.Predicate(dim, values)
    }

    private fun readIdent(): String {
        val start = pos
        while (pos < src.length) {
            val c = src[pos]
            if (c.isLetterOrDigit() || c == '_') pos++ else break
        }
        if (start == pos) throw FilterParseException("expected identifier at $pos")
        return src.substring(start, pos)
    }

    private fun readValue(): String {
        skipWs()
        if (peek() == '"') {
            pos++
            val start = pos
            while (pos < src.length && src[pos] != '"') pos++
            if (pos >= src.length) throw FilterParseException("unterminated string literal")
            val v = src.substring(start, pos)
            pos++
            return v
        }
        val start = pos
        while (pos < src.length) {
            val c = src[pos]
            if (c.isWhitespace() || c == ',' || c == ')' || c == '(') break
            pos++
        }
        if (start == pos) throw FilterParseException("expected value at $pos")
        return src.substring(start, pos)
    }

    private fun matchKeyword(k: String): Boolean {
        skipWs()
        val end = pos + k.length
        if (end > src.length) return false
        if (!src.substring(pos, end).equals(k, ignoreCase = true)) return false
        if (end < src.length && (src[end].isLetterOrDigit() || src[end] == '_')) return false
        pos = end
        return true
    }

    private fun peek(): Char = if (pos < src.length) src[pos] else 0.toChar()

    private fun skipWs() {
        while (pos < src.length && src[pos].isWhitespace()) pos++
    }
}
