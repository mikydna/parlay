from pathlib import Path

import pytest

from prop_ev.latex_renderer import compile_pdf, markdown_to_latex, render_pdf_from_markdown


def test_markdown_to_latex_basic_sections() -> None:
    markdown = "# Strategy Brief\n\n## Snapshot\n\n### Game Header\n\n- one\n- two\n"
    tex = markdown_to_latex(markdown, title="NBA Strategy Brief")
    assert "\\section*{Strategy Brief}" in tex
    assert "\\subsection*{Snapshot}" in tex
    assert "\\subsubsection*{Game Header}" in tex
    assert "\\begin{itemize}" in tex


def test_markdown_to_latex_landscape() -> None:
    markdown = "# Strategy Brief\n"
    tex = markdown_to_latex(markdown, title="NBA Strategy Brief", landscape=True)
    assert "\\usepackage[margin=1in,landscape]{geometry}" in tex


def test_markdown_to_latex_table() -> None:
    markdown = (
        "## Action Plan (GO / LEAN / NO-GO)\n\n"
        "| Action | Bet Type | Ticket | EV | Kelly | Why |\n"
        "| --- | --- | --- | ---: | ---: | --- |\n"
        "| LEAN | player_prop | A UNDER 10.5 points @ +100 (book) | 0.05 | 0.03 | note |\n"
    )
    tex = markdown_to_latex(markdown, title="NBA Strategy Brief")
    assert "\\begin{longtable}" in tex
    assert "\\endfirsthead" in tex
    assert "\\textbf{Action}" in tex
    assert "A UNDER 10.5 points @ +100 (book)" in tex


def test_markdown_to_latex_pagebreak_marker() -> None:
    markdown = "## First\n\n- one\n\n<!-- pagebreak -->\n\n## Second\n\n- two\n"
    tex = markdown_to_latex(markdown, title="NBA Strategy Brief")
    assert "\\newpage" in tex


def test_markdown_to_latex_inline_bold_code_and_links() -> None:
    markdown = (
        "## Analyst Take\n\n"
        "- **Best Bet:** **Jay Huff OVER 13.5 points @ +100 (fanduel)**\n"
        "- Lookup: `IND @ BKN | OVER 13.5 points | fanduel +100`\n"
        "- Source: [StatsArc](https://www.statsarc.com/a_b)\n"
    )
    tex = markdown_to_latex(markdown, title="NBA Strategy Brief")
    assert "\\textbf{Best Bet:}" in tex
    assert "\\textbf{Jay Huff OVER 13.5 points @ +100 (fanduel)}" in tex
    assert "\\texttt{IND @ BKN | OVER 13.5 points | fanduel +100}" in tex
    assert "\\href{https://www.statsarc.com/a\\_b}{StatsArc}" in tex


def test_compile_pdf_missing_tool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tex_path = tmp_path / "brief.tex"
    tex_path.write_text(
        "\\documentclass{article}\\begin{document}x\\end{document}", encoding="utf-8"
    )
    pdf_path = tmp_path / "brief.pdf"

    monkeypatch.setattr("prop_ev.latex_renderer.shutil.which", lambda _: None)
    result = compile_pdf(tex_path=tex_path, pdf_path=pdf_path)
    assert result["status"] == "missing_tool"


def test_render_pdf_from_markdown_writes_tex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("prop_ev.latex_renderer.shutil.which", lambda _: None)
    tex_path = tmp_path / "strategy-brief.tex"
    pdf_path = tmp_path / "strategy-brief.pdf"
    result = render_pdf_from_markdown(
        "# Strategy Brief\n\n## Snapshot\n\n- test\n",
        tex_path=tex_path,
        pdf_path=pdf_path,
        title="NBA Strategy Brief",
    )
    assert tex_path.exists()
    assert result["status"] == "missing_tool"
