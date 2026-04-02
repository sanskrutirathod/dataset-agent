"""Web crawler module: BFS link following with robots.txt, sitemap, rate limiting, and dedup."""
from __future__ import annotations
import hashlib
import logging
import time
from collections import deque
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
from requests.adapters import HTTPAdapter, Retry

from ..schemas import Source
from ..utils.ids import new_id
from .ingest import _is_safe_url, _html_to_text, HEADERS

logger = logging.getLogger(__name__)


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def _normalize_url(url: str) -> str:
    """Normalize URL: strip fragment, lowercase scheme+host."""
    try:
        p = urlparse(url)
        normalized = urlunparse((
            p.scheme.lower(),
            p.netloc.lower(),
            p.path,
            p.params,
            p.query,
            "",  # strip fragment
        ))
        return normalized.rstrip("/") if normalized.endswith("/") and len(p.path) > 1 else normalized
    except Exception:
        return url


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def _get_domain(url: str) -> str:
    return urlparse(url).netloc.lower()


def _fetch_robots(domain: str, scheme: str, session: requests.Session) -> RobotFileParser:
    rp = RobotFileParser()
    robots_url = f"{scheme}://{domain}/robots.txt"
    try:
        resp = session.get(robots_url, timeout=10)
        if resp.status_code == 200:
            rp.parse(resp.text.splitlines())
        else:
            rp.allow_all = True
    except Exception:
        rp.allow_all = True
    return rp


def _parse_sitemap(url: str, session: requests.Session) -> list[str]:
    """Parse a sitemap (or sitemap index) and return all page URLs found."""
    urls: list[str] = []
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return urls
        content = resp.text
        from xml.etree import ElementTree as ET
        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            return urls

        ns = ""
        tag = root.tag
        if tag.startswith("{"):
            ns = tag[:tag.index("}") + 1]

        if root.tag in (f"{ns}sitemapindex", "sitemapindex"):
            for sitemap_elem in root.findall(f"{ns}sitemap"):
                loc = sitemap_elem.find(f"{ns}loc")
                if loc is not None and loc.text:
                    sub_urls = _parse_sitemap(loc.text.strip(), session)
                    urls.extend(sub_urls)
        else:
            for url_elem in root.findall(f"{ns}url"):
                loc = url_elem.find(f"{ns}loc")
                if loc is not None and loc.text:
                    urls.append(loc.text.strip())
    except Exception as e:
        logger.debug(f"Sitemap parse failed for {url}: {e}")
    return urls


def _extract_links(html: str, base_url: str) -> list[str]:
    """Extract all href links from HTML."""
    links: list[str] = []
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("javascript:"):
                continue
            abs_url = urljoin(base_url, href)
            links.append(abs_url)
    except Exception as e:
        logger.debug(f"Link extraction failed for {base_url}: {e}")
    return links


class WebCrawler:
    """BFS web crawler with robots.txt, sitemap, rate limiting, and dedup."""

    def __init__(
        self,
        seed_url: str,
        max_depth: int = 2,
        max_pages: int = 50,
        allowed_domains: Optional[list[str]] = None,
        delay_ms: int = 500,
    ):
        self.seed_url = seed_url
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.allowed_domains = [d.lower().strip() for d in allowed_domains] if allowed_domains else None
        self.delay_ms = delay_ms
        self.session = _make_session()
        self._robots_cache: dict[str, RobotFileParser] = {}
        self._last_fetch_time: dict[str, float] = {}

    def _get_robots(self, domain: str, scheme: str) -> RobotFileParser:
        if domain not in self._robots_cache:
            self._robots_cache[domain] = _fetch_robots(domain, scheme, self.session)
        return self._robots_cache[domain]

    def _is_allowed_domain(self, url: str) -> bool:
        domain = _get_domain(url)
        if self.allowed_domains:
            return any(domain == d or domain.endswith("." + d) for d in self.allowed_domains)
        seed_domain = _get_domain(self.seed_url)
        return domain == seed_domain

    def _rate_limit(self, domain: str) -> None:
        last = self._last_fetch_time.get(domain, 0.0)
        elapsed_ms = (time.time() - last) * 1000
        if elapsed_ms < self.delay_ms:
            time.sleep((self.delay_ms - elapsed_ms) / 1000.0)
        self._last_fetch_time[domain] = time.time()

    def _can_fetch(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        scheme = parsed.scheme
        rp = self._get_robots(domain, scheme)
        user_agent = HEADERS.get("User-Agent", "*")
        try:
            return rp.can_fetch(user_agent, url)
        except Exception:
            return True

    def _fetch_page(self, url: str) -> tuple[str, str] | None:
        """Fetch a page; return (html, final_url) or None on error."""
        domain = _get_domain(url)
        self._rate_limit(domain)
        try:
            resp = self.session.get(url, timeout=15, allow_redirects=True)
            if resp.status_code != 200:
                return None
            content_type = resp.headers.get("content-type", "")
            if "html" not in content_type:
                return None
            return resp.text, resp.url
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
            return None

    def crawl(self, progress_callback=None) -> list[Source]:
        """Run the BFS crawl and return Source objects."""
        seed_norm = _normalize_url(self.seed_url)
        seed_parsed = urlparse(self.seed_url)
        seed_domain = seed_parsed.netloc.lower()
        seed_scheme = seed_parsed.scheme

        visited_urls: set[str] = set()
        content_hashes: set[str] = set()
        sources: list[Source] = []

        queue: deque[tuple[str, int]] = deque()

        safe, reason = _is_safe_url(self.seed_url)
        if not safe:
            raise ValueError(f"Seed URL blocked for security: {reason}")

        sitemap_url = f"{seed_scheme}://{seed_domain}/sitemap.xml"
        try:
            sitemap_urls = _parse_sitemap(sitemap_url, self.session)
            logger.info(f"Sitemap found {len(sitemap_urls)} URLs from {sitemap_url}")
            for surl in sitemap_urls:
                norm = _normalize_url(surl)
                if norm not in visited_urls and self._is_allowed_domain(surl):
                    safe_u, _ = _is_safe_url(surl)
                    if safe_u:
                        queue.append((norm, 0))
                        visited_urls.add(norm)
        except Exception as e:
            logger.debug(f"Sitemap fetch error: {e}")

        if seed_norm not in visited_urls:
            queue.appendleft((seed_norm, 0))
            visited_urls.add(seed_norm)

        while queue and len(sources) < self.max_pages:
            url, depth = queue.popleft()

            if not self._is_allowed_domain(url):
                continue

            safe_u, _ = _is_safe_url(url)
            if not safe_u:
                continue

            if not self._can_fetch(url):
                logger.debug(f"robots.txt disallows {url}")
                continue

            result = self._fetch_page(url)
            if result is None:
                continue
            html, final_url = result

            text = _html_to_text(html, final_url)
            if not text or len(text.strip()) < 50:
                continue

            text = text.encode("utf-8", errors="replace").decode("utf-8")
            chash = _content_hash(text)
            if chash in content_hashes:
                logger.debug(f"Duplicate content, skipping {url}")
                continue
            content_hashes.add(chash)

            title = _extract_page_title(html) or url.split("/")[-1] or url
            source = Source(
                source_id=new_id("src"),
                type="web",
                title=title,
                uri=final_url,
                raw_text=text,
                meta={"lang": "en", "tags": [], "url": final_url, "crawl_depth": depth},
            )
            sources.append(source)
            logger.info(f"Crawled ({len(sources)}/{self.max_pages}): {url}")

            if progress_callback:
                progress_callback(len(sources), seed_domain)

            if depth < self.max_depth:
                links = _extract_links(html, final_url)
                for link in links:
                    norm_link = _normalize_url(link)
                    if norm_link not in visited_urls and self._is_allowed_domain(link):
                        safe_l, _ = _is_safe_url(link)
                        if safe_l:
                            visited_urls.add(norm_link)
                            queue.append((norm_link, depth + 1))

        logger.info(f"Crawl complete: {len(sources)} pages from {seed_domain}")
        return sources


def _extract_page_title(html: str) -> str:
    """Extract page title from HTML <title> tag."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        if title_tag and title_tag.string:
            return title_tag.string.strip()
    except Exception:
        pass
    return ""


def crawl_site(
    seed_url: str,
    max_depth: int = 2,
    max_pages: int = 50,
    allowed_domains: Optional[list[str]] = None,
    delay_ms: int = 500,
    progress_callback=None,
) -> list[Source]:
    """Convenience function to crawl a site and return Source objects."""
    crawler = WebCrawler(
        seed_url=seed_url,
        max_depth=max_depth,
        max_pages=max_pages,
        allowed_domains=allowed_domains,
        delay_ms=delay_ms,
    )
    return crawler.crawl(progress_callback=progress_callback)
