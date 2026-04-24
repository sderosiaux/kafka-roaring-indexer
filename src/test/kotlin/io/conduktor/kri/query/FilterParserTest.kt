package io.conduktor.kri.query

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class FilterParserTest {
    @Test
    fun `empty filter is True`() {
        assertThat(FilterParser.parse(null)).isEqualTo(FilterAst.True)
        assertThat(FilterParser.parse("")).isEqualTo(FilterAst.True)
        assertThat(FilterParser.parse("   ")).isEqualTo(FilterAst.True)
    }

    @Test
    fun `single predicate`() {
        val ast = FilterParser.parse("country:FR")
        assertThat(ast).isInstanceOf(FilterAst.Predicate::class.java)
        val p = ast as FilterAst.Predicate
        assertThat(p.dim).isEqualTo("country")
        assertThat(p.values).containsExactly("FR")
    }

    @Test
    fun `multi-value predicate`() {
        val ast = FilterParser.parse("country:FR,DE,IT") as FilterAst.Predicate
        assertThat(ast.values).containsExactly("FR", "DE", "IT")
    }

    @Test
    fun `AND composition`() {
        val ast = FilterParser.parse("country:FR AND device:mobile")
        assertThat(ast).isInstanceOf(FilterAst.And::class.java)
    }

    @Test
    fun `OR and NOT with parens`() {
        val ast = FilterParser.parse("(country:FR OR country:DE) AND NOT status:500")
        assertThat(ast).isInstanceOf(FilterAst.And::class.java)
        val and = ast as FilterAst.And
        assertThat(and.left).isInstanceOf(FilterAst.Or::class.java)
        assertThat(and.right).isInstanceOf(FilterAst.Not::class.java)
    }

    @Test
    fun `quoted value`() {
        val ast = FilterParser.parse("path:\"/api/v1/foo bar\"")
        assertThat((ast as FilterAst.Predicate).values).containsExactly("/api/v1/foo bar")
    }

    @Test
    fun `keywords are case-insensitive`() {
        val ast = FilterParser.parse("country:FR and device:mobile or NOT status:500")
        // Grammar: OR has lowest precedence so "or" should yield Or at root.
        assertThat(ast).isInstanceOf(FilterAst.Or::class.java)
    }

    @Test
    fun `bad grammar throws`() {
        assertThatThrownBy { FilterParser.parse("country FR") }.isInstanceOf(FilterParseException::class.java)
        assertThatThrownBy { FilterParser.parse("country:") }.isInstanceOf(FilterParseException::class.java)
        assertThatThrownBy { FilterParser.parse("(country:FR") }.isInstanceOf(FilterParseException::class.java)
    }
}
