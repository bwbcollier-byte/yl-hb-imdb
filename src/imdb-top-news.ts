import * as dotenv from 'dotenv';
dotenv.config();

import { supabase } from './supabase';
// puppeteer-extra has limited TS support — use require for stealth plugin
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

import type { Browser, Page } from 'puppeteer';

const MAX_PAGES = parseInt(process.env.MAX_PAGES || '5', 10);
const BATCH_SIZE = 20;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Article {
    title: string;
    text: string;
    source: string | null;
    published: string | null;
    link: string;
    img: string | null;
    imgNote: string | null;
    relatedIds: string[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseDateStr(dateStr: string | null): Date | null {
    if (!dateStr) return null;
    const d = new Date(dateStr);
    if (!isNaN(d.valueOf())) return d;

    if (dateStr.includes('ago')) {
        const hoursMatch = dateStr.match(/(\d+)\s+hour/);
        const daysMatch  = dateStr.match(/(\d+)\s+day/);
        const minMatch   = dateStr.match(/(\d+)\s+min/);
        const now = new Date();
        if (hoursMatch) now.setHours(now.getHours()     - parseInt(hoursMatch[1]));
        if (daysMatch)  now.setDate(now.getDate()       - parseInt(daysMatch[1]));
        if (minMatch)   now.setMinutes(now.getMinutes() - parseInt(minMatch[1]));
        return now;
    }
    return null;
}

// ---------------------------------------------------------------------------
// Extract articles from the currently loaded page
// ---------------------------------------------------------------------------

async function extractArticles(page: Page): Promise<Article[]> {
    return page.evaluate((): Article[] => {
        return Array.from(document.querySelectorAll('.ipc-list-card--border-line'))
            .map((el): Article | null => {
                const titleLinkEl = el.querySelector('a[data-testid="item-text-with-link"]') as HTMLAnchorElement | null;
                if (!titleLinkEl) return null;

                const pEl       = el.querySelector('.ipc-html-content-inner-div') as HTMLElement | null;
                const listItems = Array.from(el.querySelectorAll('ul.ipc-inline-list li')) as HTMLElement[];
                const imgEl     = el.querySelector('img.ipc-image') as HTMLImageElement | null;

                const date     = listItems.length > 0 ? listItems[0].innerText : null;
                const fullText = pEl ? pEl.innerText : '';
                const srcMatch = fullText.match(/See full article at (.+)/);

                const rawSrc = imgEl
                    ? (imgEl.getAttribute('src') || imgEl.getAttribute('data-src') || null)
                    : null;

                let imgUrl: string | null  = rawSrc;
                let imgNote: string | null = null;

                if (!rawSrc) {
                    imgNote = 'image:no_thumbnail';
                } else if (rawSrc.includes('._V1_')) {
                    const crMatch = rawSrc.match(/_CR\d+,\d+,(\d+),(\d+)_/);
                    if (crMatch) {
                        const ratio = parseInt(crMatch[1]) / parseInt(crMatch[2]);
                        if (ratio <= 1.05) imgNote = 'image:headshot_fallback';
                    }
                    imgUrl = rawSrc.replace(/\._V1_.*?(\.\w+)$/, '._V1_UX1200$1');
                }

                const relatedIds: string[] = Array.from(pEl ? pEl.querySelectorAll('a') : [])
                    .map((l: Element) => {
                        const m = (l as HTMLAnchorElement).href.match(/\/(nm\d+|tt\d+)\/?/);
                        return m ? m[1] : null;
                    })
                    .filter((id): id is string => id !== null);

                return {
                    title:      titleLinkEl.innerText.trim(),
                    text:       fullText,
                    source:     srcMatch ? srcMatch[1].trim() : null,
                    published:  date,
                    link:       titleLinkEl.href.split('?')[0],
                    img:        imgUrl,
                    imgNote,
                    relatedIds: [...new Set(relatedIds)],
                };
            })
            .filter((a): a is Article => a !== null);
    });
}

// ---------------------------------------------------------------------------
// Batch DB lookups
// ---------------------------------------------------------------------------

async function buildNmLookup(nmIds: string[]): Promise<Record<string, string>> {
    if (nmIds.length === 0) return {};
    const lookup: Record<string, string> = {};
    for (let i = 0; i < nmIds.length; i += 100) {
        const batch = nmIds.slice(i, i + 100);
        const { data } = await supabase
            .from('hb_socials')
            .select('identifier, linked_talent')
            .in('identifier', batch)
            .not('linked_talent', 'is', null);
        if (data) {
            data.forEach((s: any) => { if (s.linked_talent) lookup[s.identifier] = s.linked_talent; });
        }
    }
    return lookup;
}

async function buildTtLookup(ttIds: string[]): Promise<Record<string, string>> {
    if (ttIds.length === 0) return {};
    const lookup: Record<string, string> = {};
    for (let i = 0; i < ttIds.length; i += 100) {
        const batch = ttIds.slice(i, i + 100);
        const { data } = await supabase
            .from('hb_media')
            .select('id, soc_imdb_id')
            .in('soc_imdb_id', batch);
        if (data) {
            data.forEach((m: any) => { if (m.soc_imdb_id) lookup[m.soc_imdb_id] = m.id; });
        }
    }
    return lookup;
}

async function getExistingLinks(links: string[]): Promise<Set<string>> {
    const existing = new Set<string>();
    for (let i = 0; i < links.length; i += 100) {
        const batch = links.slice(i, i + 100);
        const { data } = await supabase
            .from('news')
            .select('source_link')
            .in('source_link', batch);
        if (data) data.forEach((row: any) => existing.add(row.source_link));
    }
    return existing;
}

// ---------------------------------------------------------------------------
// Main Pipeline
// ---------------------------------------------------------------------------

async function run(): Promise<void> {
    const startTime = Date.now();
    console.log(`\n📰 IMDb Top News Scraper`);
    console.log(`   Max "50 more" clicks: ${MAX_PAGES}`);
    console.log('─'.repeat(60));

    const browser: Browser = await puppeteer.launch({
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });
    const page: Page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });

    // Block heavy resources to speed up loading
    await page.setRequestInterception(true);
    page.on('request', (req: any) => {
        if (['stylesheet', 'font', 'media'].includes(req.resourceType())) {
            req.abort();
        } else {
            req.continue();
        }
    });

    try {
        // 1. Load the top news page
        console.log('\n🌐 Loading https://www.imdb.com/news/top/ ...');
        await page.goto('https://www.imdb.com/news/top/', { waitUntil: 'networkidle2', timeout: 60000 });

        // Wait for article cards to appear
        const hasContent = await page
            .waitForSelector('.ipc-list-card--border-line', { timeout: 15000 })
            .then(() => true)
            .catch(() => false);

        if (!hasContent) {
            console.log('❌ No article cards found on page — possible bot block or layout change.');
            await browser.close();
            return;
        }

        // 2. Click "50 more" button to load additional articles
        for (let click = 1; click <= MAX_PAGES; click++) {
            try {
                const prevCount: number = await page.$$eval(
                    '.ipc-list-card--border-line', (els: Element[]) => els.length
                );

                // Use page.evaluate to find, scroll, and click — avoids Puppeteer's
                // "not clickable" error on dynamically rendered buttons.
                const clicked: boolean = await page.evaluate(() => {
                    const btn = document.querySelector('button.ipc-see-more__button') as HTMLElement | null;
                    if (!btn) return false;
                    btn.scrollIntoView({ block: 'center' });
                    btn.click();
                    return true;
                });

                if (!clicked) {
                    console.log(`   ℹ️  No more button found after ${click - 1} clicks.`);
                    break;
                }

                console.log(`   📄 Clicked "50 more" (${click}/${MAX_PAGES}) — had ${prevCount} articles...`);

                // Wait for new cards to appear
                await page.waitForFunction(
                    (n: number) => document.querySelectorAll('.ipc-list-card--border-line').length > n,
                    { timeout: 10000 },
                    prevCount
                ).catch(() => {
                    console.log(`   ℹ️  No new articles loaded after click ${click}.`);
                });
            } catch (e: any) {
                console.log(`   ℹ️  Pagination ended after ${click - 1} clicks: ${e.message}`);
                break;
            }
        }

        // 3. Extract all visible articles and deduplicate by source_link
        const rawArticles = await extractArticles(page);
        const seenLinks = new Set<string>();
        const allArticles = rawArticles.filter(a => {
            if (!a.link || seenLinks.has(a.link)) return false;
            seenLinks.add(a.link);
            return true;
        });
        console.log(`\n📦 Extracted ${rawArticles.length} articles (${allArticles.length} unique) from the page.`);

        // 4. Filter out articles we already have
        const allLinks = allArticles.map(a => a.link).filter(Boolean);
        const existingLinks = await getExistingLinks(allLinks);
        const newArticles = allArticles.filter(a => a.link && !existingLinks.has(a.link));
        console.log(`   ${allArticles.length - newArticles.length} already in database. ${newArticles.length} new to process.`);

        if (newArticles.length === 0) {
            console.log('\n✅ No new articles to insert.');
            await browser.close();
            return;
        }

        // 5. Build lookup maps for nm/tt IDs → UUIDs
        const allNmIds = [...new Set(newArticles.flatMap(a => a.relatedIds.filter(id => id.startsWith('nm'))))];
        const allTtIds = [...new Set(newArticles.flatMap(a => a.relatedIds.filter(id => id.startsWith('tt'))))];

        console.log(`\n🔗 Cross-referencing ${allNmIds.length} talent IDs and ${allTtIds.length} media IDs...`);
        const [nmMap, ttMap] = await Promise.all([buildNmLookup(allNmIds), buildTtLookup(allTtIds)]);
        console.log(`   Found ${Object.keys(nmMap).length} talent matches, ${Object.keys(ttMap).length} media matches.`);

        // 6. Insert new articles in batches
        let insertedCount = 0;
        let errorCount = 0;

        for (let i = 0; i < newArticles.length; i += BATCH_SIZE) {
            const batch = newArticles.slice(i, i + BATCH_SIZE);
            const payloads = batch.map(item => {
                const pubDate = parseDateStr(item.published)?.toISOString() ?? null;
                const rawNmIds = item.relatedIds.filter(id => id.startsWith('nm'));
                const rawTtIds = item.relatedIds.filter(id => id.startsWith('tt'));
                const talentUuids = [...new Set(rawNmIds.map(id => nmMap[id]).filter(Boolean))];
                const mediaUuids = rawTtIds.map(id => ttMap[id]).filter(Boolean);

                return {
                    article_title:     item.title,
                    article:           item.text,
                    source_name:       item.source || 'IMDb',
                    source_link:       item.link,
                    source_favicon:    'https://m.media-amazon.com/images/G/01/imdb/images-ANDW73HA/favicon_desktop_32x32._CB1582158068_.png',
                    image_primary:     item.img,
                    published:         pubDate,
                    status:            'published',
                    public_visible:    true,
                    tagged_talent:     talentUuids,
                    tagged_media:      mediaUuids,
                    linked_talent_ids: rawNmIds,
                    linked_media_ids:  rawTtIds,
                    internal_notes:    item.imgNote ? [item.imgNote] : [],
                };
            });

            const { error } = await supabase.from('news').upsert(payloads, { onConflict: 'source_link' });
            if (error) {
                console.error(`   ❌ Batch insert error:`, error.message);
                errorCount += batch.length;
            } else {
                insertedCount += batch.length;
                console.log(`   ✅ Inserted batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} articles)`);
            }
        }

        const durationSecs = Math.round((Date.now() - startTime) / 1000);
        console.log('\n' + '─'.repeat(60));
        console.log(`🏁 Done in ${durationSecs}s! Inserted: ${insertedCount} | Errors: ${errorCount} | Skipped: ${allArticles.length - newArticles.length}`);

    } catch (e: any) {
        console.error('💥 Fatal error:', e.message);
    } finally {
        await browser.close();
    }
}

run().catch(e => {
    console.error('Fatal:', e);
    process.exit(1);
});
