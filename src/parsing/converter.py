from markdownify import MarkdownConverter

from common.markdown import remove_excessive_n, ignore_asterics, remove_weird_ending, fix_brain_cancer, \
    MarkdownPost, remove_excessive_space


def chomp(text):
    """
    If the text in an inline tag like b, a, or em contains a leading or trailing
    space, strip the string and return a space as suffix of prefix, if needed.
    This function is used to prevent conversions like
        <b> foo</b> => ** foo**
    """
    prefix = ' ' if text and text[0] == ' ' else ''
    suffix = ' ' if text and text[-1] == ' ' else ''
    # text = text.strip()
    return (prefix, suffix, text)


def abstract_inline_conversion(markup_fn):
    """
    This abstracts all simple inline tags like b, em, del, ...
    Returns a function that wraps the chomped text in a pair of the string
    that is returned by markup_fn. markup_fn is necessary to allow for
    references to self.strong_em_symbol etc.
    """

    def implementation(self, el, text, convert_as_inline):
        markup = markup_fn(self)
        prefix, suffix, text = chomp(text)
        if not text:
            return ''
        return '%s%s%s%s%s' % (prefix, markup, text, markup, suffix)

    return implementation


class ToTelegramMarkdown(MarkdownConverter):
    _convert_strong = abstract_inline_conversion(
        lambda self: self.options.get('bold_symbol', "**")
    )
    _convert_i = abstract_inline_conversion(
        lambda self: self.options.get("italic_symbol", "")  # ignore
    )
    _convert_em = _convert_i

    def __init__(self, **options):
        options["escape_underscores"] = options.get("escape_underscores", False)

        self.ignore = options.get("ignore", [])
        self.handle_link = options.get("handle_link")
        super(ToTelegramMarkdown, self).__init__(**options)

    def convert_img(self, *args, **kwargs):
        return ""

    def convert_br(self, *args, **kwargs):
        return "\n"

    def convert_table(self, el, text, convert_as_inline):
        return ""

    def convert_a(self, el, text, convert_as_inline):
        prefix, suffix, text = chomp(text)
        if not text:
            return ''
        href = el.get('href')
        title = el.get('title')
        href = self.handle_link(href) if self.handle_link else href

        # For the replacement see #29: text nodes underscores are escaped
        if (self.options['autolinks']
                and text.replace(r'\_', '_') == href
                and not title
                and not self.options['default_title']):
            # Shortcut syntax
            return '%s' % href
        if self.options['default_title'] and not title:
            title = href
        title_part = ' "%s"' % title.replace('"', r'\"') if title else ''
        return '%s[%s](%s%s)%s' % (prefix, text, href, title_part, suffix) if href else text

    def convert_i(self, el, text, convert_as_inline):
        if el.previous_element and el.previous_element.name == "i":
            return text

        return self._convert_i(el, text, convert_as_inline)

    def convert_em(self, el, text, convert_as_inline):
        if el.previous_element and el.previous_element.name == "em":
            return text

        return self._convert_em(el, text, convert_as_inline)

    def convert_strong(self, el, text, convert_as_inline):
        if el.previous_element and el.previous_element.name == "strong":
            return text

        text = self._convert_strong(el, text, convert_as_inline)
        for sibling in getattr(el, "previous_siblings"):
            if not sibling.name:
                continue

            if sibling.name == "ul":
                text = "\n" + text
            else:
                break

        return text

    def convert(self, html):
        text = super().convert(html)
        text = remove_excessive_space(text)
        text = remove_excessive_n(text)
        text = remove_weird_ending(text)
        return MarkdownPost(text.strip())

    # convert_ol
    # convert_ul
    # underline
    # convert_a
    # convert_li
    # convert_p

    def convert_p(self, *args, **kwargs):
        text = super().convert_p(*args, **kwargs)
        # return text
        # if text.endswith("\n\n"):
        #     text = text[:-1]
        return "\n" + text

    def process_tag(self, node, convert_as_inline, children_only=False):
        if node.name == "style": return ""
        if node.name == "img": return ""
        if node.name == "header": return ""
        if node.name == "script": return ""
        if node.name == "footer": return ""
        if node.name == "iframe": return ""

        text = super().process_tag(node, convert_as_inline, children_only)
        return text

    def convert_list(self, el, text, convert_as_inline):

        # Converting a list to inline is undefined.
        # Ignoring convert_to_inline for list.

        nested = False
        before_paragraph = False
        if el.next_sibling and el.next_sibling.name not in ['ul', 'ol']:
            before_paragraph = True
        while el:
            if el.name == 'li':
                nested = True
                break
            el = el.parent
        if nested:
            # remove trailing newline if nested
            return '\n' + self.indent(text, 1).rstrip()
        return "\n" + text + ('\n' if before_paragraph else '')

    def process_text(self, el):
        if len(el.replace(" ", "")) == 0:
            return ""

        if el.strip() in self.ignore:
            return ""

        text = super().process_text(el)
        text = ignore_asterics(text)
        return text

    def convert_hr(self, *args, **kwargs):
        return ""

    def convert_ul(self, el, text, convert_as_inline):
        if len(text.strip()) == 0:
            return ""

        return self.convert_list(el, text.strip(), convert_as_inline)

    def convert_hn(self, n, el, text, convert_as_inline):
        text = self.convert_strong(el, text, convert_as_inline)
        if len(text.strip()) == 0:
            return text

        return "\n\n" + text + "\n\n"
