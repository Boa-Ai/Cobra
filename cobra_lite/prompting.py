from functools import lru_cache

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from cobra_lite.config import PROMPTS_DIR


@lru_cache(maxsize=1)
def _prompt_env() -> Environment:
    if not PROMPTS_DIR.exists():
        raise FileNotFoundError(f"Prompt directory does not exist: {PROMPTS_DIR}")
    return Environment(
        loader=FileSystemLoader(str(PROMPTS_DIR)),
        autoescape=False,
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
        undefined=StrictUndefined,
    )


def render_prompt(template_name: str, **context: object) -> str:
    template = _prompt_env().get_template(template_name)
    return template.render(**context).strip()
