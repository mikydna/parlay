"""Render markdown strategy briefs into LaTeX and optional PDF."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

INLINE_TOKEN_RE = re.compile(r"\*\*([^*]+)\*\*|`([^`]+)`|\[([^\]]+)\]\(([^)]+)\)")


def escape_latex(text: str) -> str:
    """Escape special LaTeX characters."""
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(ch, ch) for ch in text)


def _render_inline_markdown(text: str) -> str:
    """Convert a small subset of inline markdown to LaTeX-safe inline text."""
    rendered: list[str] = []
    cursor = 0
    for match in INLINE_TOKEN_RE.finditer(text):
        start, end = match.span()
        if start > cursor:
            rendered.append(escape_latex(text[cursor:start]))
        bold_text = match.group(1)
        code_text = match.group(2)
        link_label = match.group(3)
        link_url = match.group(4)
        if bold_text is not None:
            rendered.append(rf"\textbf{{{_render_inline_markdown(bold_text)}}}")
        elif code_text is not None:
            rendered.append(rf"\texttt{{{escape_latex(code_text)}}}")
        elif link_label is not None and link_url is not None:
            rendered.append(
                rf"\href{{{escape_latex(link_url)}}}{{{_render_inline_markdown(link_label)}}}"
            )
        cursor = end
    if cursor < len(text):
        rendered.append(escape_latex(text[cursor:]))
    return "".join(rendered)


def _split_table_row(line: str) -> list[str] | None:
    stripped = line.strip()
    if not (stripped.startswith("|") and stripped.endswith("|")):
        return None
    placeholder = "__PIPE_PLACEHOLDER__"
    normalized = stripped.replace("\\|", placeholder)
    parts = [cell.strip().replace(placeholder, "|") for cell in normalized[1:-1].split("|")]
    if not parts:
        return None
    return parts


def _is_separator_cell(value: str) -> bool:
    raw = value.strip()
    if not raw:
        return False
    if "-" not in raw:
        return False
    return bool(re.fullmatch(r":?-{3,}:?", raw))


def _is_table_separator(line: str, column_count: int) -> bool:
    cells = _split_table_row(line)
    if not cells or len(cells) != column_count:
        return False
    return all(_is_separator_cell(cell) for cell in cells)


def _table_colspec(headers: list[str]) -> str:
    specs: list[str] = []
    for header in headers:
        label = header.strip().lower()
        if label in {"ev", "kelly"}:
            specs.append("r")
        elif label in {"ticket", "why", "game", "edge note"}:
            specs.append("Y")
        else:
            specs.append("l")
    return "|".join(specs)


def _render_table_latex(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines: list[str] = []
    colspec = _table_colspec(headers)
    lines.append(rf"\begin{{tabularx}}{{\textwidth}}{{|{colspec}|}}")
    lines.append(r"\hline")
    header_cells = [rf"\textbf{{{_render_inline_markdown(cell)}}}" for cell in headers]
    header_row = " & ".join(header_cells) + r" \\"
    lines.append(header_row)
    lines.append(r"\hline")
    for row in rows:
        padded = row[: len(headers)] + [""] * max(0, len(headers) - len(row))
        row_cells = [_render_inline_markdown(cell) for cell in padded[: len(headers)]]
        row_line = " & ".join(row_cells) + r" \\"
        lines.append(row_line)
        lines.append(r"\hline")
    lines.append(r"\end{tabularx}")
    return lines


def markdown_to_latex(
    markdown: str,
    *,
    title: str = "Strategy Brief",
    landscape: bool = False,
) -> str:
    """Convert simple markdown to a deterministic LaTeX document."""
    lines = markdown.splitlines()
    geometry = (
        r"\usepackage[margin=1in,landscape]{geometry}"
        if landscape
        else r"\usepackage[margin=1in]{geometry}"
    )
    out: list[str] = [
        r"\documentclass[10pt]{article}",
        geometry,
        r"\usepackage[T1]{fontenc}",
        r"\usepackage[utf8]{inputenc}",
        r"\usepackage{lmodern}",
        r"\usepackage{hyperref}",
        r"\usepackage{tabularx}",
        r"\usepackage{array}",
        r"\usepackage{ragged2e}",
        r"\newcolumntype{Y}{>{\RaggedRight\arraybackslash}X}",
        r"\renewcommand{\arraystretch}{1.05}",
        r"\setlength{\parskip}{4pt}",
        r"\setlength{\parindent}{0pt}",
        r"\sloppy",
        r"\begin{document}",
        rf"\begin{{center}}\LARGE\textbf{{{escape_latex(title)}}}\end{{center}}",
        r"\vspace{0.5em}",
    ]

    in_list = False
    idx = 0
    while idx < len(lines):
        raw_line = lines[idx]
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if in_list:
                out.append(r"\end{itemize}")
                in_list = False
            out.append("")
            idx += 1
            continue

        if stripped.lower() in {"<!-- pagebreak -->", "<pagebreak>", "[pagebreak]", r"\newpage"}:
            if in_list:
                out.append(r"\end{itemize}")
                in_list = False
            out.append(r"\newpage")
            idx += 1
            continue

        if stripped.startswith("# "):
            if in_list:
                out.append(r"\end{itemize}")
                in_list = False
            out.append(rf"\section*{{{_render_inline_markdown(stripped[2:].strip())}}}")
            idx += 1
            continue

        if stripped.startswith("## "):
            if in_list:
                out.append(r"\end{itemize}")
                in_list = False
            out.append(rf"\subsection*{{{_render_inline_markdown(stripped[3:].strip())}}}")
            idx += 1
            continue

        if stripped.startswith("### "):
            if in_list:
                out.append(r"\end{itemize}")
                in_list = False
            out.append(rf"\subsubsection*{{{_render_inline_markdown(stripped[4:].strip())}}}")
            idx += 1
            continue

        header_cells = _split_table_row(stripped)
        if (
            header_cells
            and idx + 1 < len(lines)
            and _is_table_separator(lines[idx + 1], len(header_cells))
        ):
            if in_list:
                out.append(r"\end{itemize}")
                in_list = False
            table_rows: list[list[str]] = []
            row_idx = idx + 2
            while row_idx < len(lines):
                row_cells = _split_table_row(lines[row_idx].strip())
                if row_cells is None:
                    break
                if len(row_cells) != len(header_cells):
                    break
                table_rows.append(row_cells)
                row_idx += 1
            out.extend(_render_table_latex(header_cells, table_rows))
            out.append("")
            idx = row_idx
            continue

        if stripped.startswith("- "):
            if not in_list:
                out.append(r"\begin{itemize}")
                in_list = True
            out.append(rf"\item {_render_inline_markdown(stripped[2:].strip())}")
            idx += 1
            continue

        if in_list:
            out.append(r"\end{itemize}")
            in_list = False
        out.append(_render_inline_markdown(stripped))
        idx += 1

    if in_list:
        out.append(r"\end{itemize}")

    out.append(r"\end{document}")
    return "\n".join(out) + "\n"


def write_latex(
    markdown: str,
    *,
    tex_path: Path,
    title: str = "Strategy Brief",
    landscape: bool = False,
) -> Path:
    """Write LaTeX source file from markdown."""
    tex_path.parent.mkdir(parents=True, exist_ok=True)
    tex_path.write_text(
        markdown_to_latex(markdown, title=title, landscape=landscape),
        encoding="utf-8",
    )
    return tex_path


def compile_pdf(*, tex_path: Path, pdf_path: Path) -> dict[str, Any]:
    """Compile LaTeX into PDF with tectonic when available."""
    tectonic = shutil.which("tectonic")
    if not tectonic:
        return {
            "status": "missing_tool",
            "message": "tectonic not found on PATH",
            "pdf_path": str(pdf_path),
        }

    cmd = [tectonic, str(tex_path), "--outdir", str(tex_path.parent)]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    default_pdf = tex_path.with_suffix(".pdf")
    if proc.returncode != 0:
        return {
            "status": "failed",
            "message": (proc.stderr or proc.stdout).strip()[-1000:],
            "pdf_path": str(pdf_path),
            "returncode": proc.returncode,
        }
    if not default_pdf.exists():
        return {
            "status": "failed",
            "message": "tectonic completed but pdf file is missing",
            "pdf_path": str(pdf_path),
            "returncode": proc.returncode,
        }
    if default_pdf != pdf_path:
        pdf_path.write_bytes(default_pdf.read_bytes())
    return {
        "status": "ok",
        "message": "pdf generated",
        "pdf_path": str(pdf_path),
        "returncode": proc.returncode,
    }


def render_pdf_from_markdown(
    markdown: str,
    *,
    tex_path: Path,
    pdf_path: Path,
    title: str = "Strategy Brief",
    landscape: bool = False,
) -> dict[str, Any]:
    """Write LaTeX and attempt PDF compilation."""
    write_latex(markdown, tex_path=tex_path, title=title, landscape=landscape)
    return compile_pdf(tex_path=tex_path, pdf_path=pdf_path)
