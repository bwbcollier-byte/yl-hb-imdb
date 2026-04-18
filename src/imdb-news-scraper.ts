import * as dotenv from 'dotenv';
dotenv.config();

import { supabase } from './supabase';
// puppeteer-extra has limited TS support — use require for stealth plugin
// eslint-disable-next-line @typescript-eslint/no-var-requires
const puppeteer = require('puppeteer-extra');
// eslint-disable-next-line @typescript-eslint/no-var-requires
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());

import type { Browser, Page } from 'puppeteer';

const CONCURRENCY    = parseInt(process.env.CONCURRENCY    || '3');
const PROFILE_LIMIT  = parseInt(process.env.PROFILE_LIMIT  || '0'); // 0 = no limit
const WORKFLOW_ID    = process.env.WORKFLOW_ID ? parseInt(process.env.WORKFLOW_ID) : null;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
async function withRetry<T>(fn: () => Promise<{ data: T; error: any }>, attempts = 3, delayMs = 3000): Promise<{ data: T; error: any }> {
    let last: { data: T; error: any } = { data: null as any, error: null };
    for (let i = 1; i <= attempts; i++) {
        if (i > 1) await sleep(delayMs);
        last = await fn();
        if (!last.error) return last;
        console.warn(`   ⚠️  Supabase attempt ${i}/${attempts} failed: ${last.error.message}`);
    }
    return last;
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface SocialProfile {
    id: string;
    identifier: string | null;
    linked_talent: string | null;
    social_url: string | null;
    checked_imdb_news: string | null;
}

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

function parseDateStrToObject(dateStr: string | null): Date | null {
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

// Inline concurrency limiter — no extra dependency needed
function createLimiter(concurrency: number) {
    let active = 0;
    const queue: (() => void)[] = [];
    return function limit<T>(fn: () => Promise<T>): Promise<T> {
        return new Promise((resolve, reject) => {
            const run = async () => {
                active++;
                try   { resolve(await fn()); }
                catch (e) { reject(e); }
                finally {
                    active--;
                    if (queue.length > 0) queue.shift()!();
                }
            };
            active < concurrency ? run() : queue.push(run);
        });
    };
}

// Notify Supabase workflow table via RPC (mirrors the existing enrichment pattern)
async function logWorkflowRun(status: string, durationSecs?: number, lastError?: string) {
    if (!WORKFLOW_ID) return;
    try {
        await supabase.rpc('log_workflow_run', {
            p_workflow_id:    WORKFLOW_ID,
            p_status:         status,
            p_duration_secs:  durationSecs ?? null,
            p_last_error:     lastError    ?? null,
        });
    } catch (_) { /* non-fatal — don't let logging failures stop the scrape */ }
}

// ---------------------------------------------------------------------------
// Per-profile scrape
// ---------------------------------------------------------------------------

async function scrapeProfile(browser: Browser, profile: SocialProfile): Promise<void> {
    let identifier = profile.identifier;
    if (!identifier && profile.social_url) {
        const m = profile.social_url.match(/name\/(nm\d+)/);
        if (m) identifier = m[1];
    }

    if (!identifier) {
        console.log(`[SKIP] No identifier for linked_talent=${profile.linked_talent}`);
        await supabase.from('hb_socials')
            .update({ checked_imdb_news: new Date().toISOString() })
            .eq('id', profile.id);
        return;
    }

    const newsUrl       = `https://www.imdb.com/name/${identifier}/news/`;
    const lastCheckTime = profile.checked_imdb_news ? new Date(profile.checked_imdb_news).valueOf() : 0;
    console.log(`[START] ${identifier} (last checked: ${profile.checked_imdb_news || 'never'})`);

    const page: Page = await browser.newPage();

    // Block resources we don't need — halves page-load time and CPU.
    // img.src is a DOM attribute set by React before the image request fires,
    // so blocking image downloads is safe for our scrape.
    await page.setRequestInterception(true);
    page.on('request', (req: any) => {
        if (['image', 'stylesheet', 'font', 'media'].includes(req.resourceType())) {
            req.abort();
        } else {
            req.continue();
        }
    });

    let articles: Article[] = [];

    try {
        await page.goto(newsUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

        // Wait for real content instead of a fixed sleep
        const hasContent = await page
            .waitForSelector('.ipc-list-card--border-line', { timeout: 12000 })
            .then(() => true)
            .catch(() => false);

        if (!hasContent) {
            console.log(`[EMPTY] ${identifier} — no news cards found, skipping.`);
            return;
        }

        // Click "load more" until we hit previously-checked dates or the limit
        let hasMore = true;
        let clicks  = 0;
        while (hasMore && clicks < 10) {
            // Stop early if the last visible article predates our last check
            if (lastCheckTime > 0) {
                const lastDateStr: string | null = await page.evaluate(() => {
                    const dates = document.querySelectorAll(
                        '.ipc-list-card--border-line ul.ipc-inline-list li:first-child'
                    );
                    return dates.length ? (dates[dates.length - 1] as HTMLElement).innerText : null;
                });
                const lastD = parseDateStrToObject(lastDateStr);
                if (lastD && lastD.valueOf() < lastCheckTime) {
                    console.log(`  [STOP] ${identifier} reached old content (${lastDateStr})`);
                    break;
                }
            }

            const moreBtn = await page.$('button.ipc-see-more__button');
            if (!moreBtn) { hasMore = false; break; }

            try {
                const isVisible: boolean = await page.evaluate((el: Element) => {
                    const r = el.getBoundingClientRect();
                    return r.top >= 0 && r.bottom <= window.innerHeight;
                }, moreBtn);
                if (!isVisible) await page.evaluate((el: Element) => (el as HTMLElement).scrollIntoView(), moreBtn);

                const prevCount: number = await page.$$eval(
                    '.ipc-list-card--border-line', (els: Element[]) => els.length
                );
                await moreBtn.click();
                console.log(`  [MORE] ${identifier} click ${clicks + 1}`);

                // Wait for new cards to appear rather than sleeping blindly
                await page.waitForFunction(
                    (n: number) => document.querySelectorAll('.ipc-list-card--border-line').length > n,
                    { timeout: 8000 },
                    prevCount
                ).catch(() => { hasMore = false; });

                clicks++;
            } catch (_) {
                hasMore = false;
            }
        }

        // Extract all article data in one browser round-trip
        articles = await page.evaluate((): Article[] => {
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

                    // img.src is the resolved attribute value — safe to read even with
                    // image requests blocked. Fall back to data-src for lazy-loaded images.
                    const rawSrc = imgEl
                        ? (imgEl.getAttribute('src') || imgEl.getAttribute('data-src') || null)
                        : null;

                    let imgUrl: string | null  = rawSrc;
                    let imgNote: string | null = null;

                    if (!rawSrc) {
                        imgNote = 'image:no_thumbnail';
                    } else if (rawSrc.includes('._V1_')) {
                        // Detect headshot fallback before we strip the params.
                        // Article thumbnails are landscape (w >> h); headshots are
                        // square or portrait. Parse the CR crop rectangle to check.
                        const crMatch = rawSrc.match(/_CR\d+,\d+,(\d+),(\d+)_/);
                        if (crMatch) {
                            const ratio = parseInt(crMatch[1]) / parseInt(crMatch[2]);
                            if (ratio <= 1.05) imgNote = 'image:headshot_fallback';
                        }
                        // UX9999: CDN returns full original (no image is 9999px wide).
                        // Safer than stripping params entirely which 404s on some paths.
                        imgUrl = rawSrc.replace(/\._V1_.*?(\.\w+)$/, '._V1_UX9999$1');
                    }

                    const relatedIds: string[] = Array.from(pEl ? pEl.querySelectorAll('a') : [])
                        .map((l: Element) => {
                            const m = (l as HTMLAnchorElement).href.match(/\/(nm\d+|tt\d+)\/?/);
                            return m ? m[1] : null;
                        })
                        .filter((id): id is string => id !== null);

                    return {
                        title:      titleLinkEl.innerText,
                        text:       fullText,
                        source:     srcMatch ? srcMatch[1].trim() : null,
                        published:  date,
                        link:       titleLinkEl.href,
                        img:        imgUrl,
                        imgNote,
                        relatedIds: [...new Set(relatedIds)],
                    };
                })
                .filter((a): a is Article => a !== null);
        });

        console.log(`  [FOUND] ${identifier}: ${articles.length} articles on page`);

        // Filter down to only articles newer than the last check
        const newArticles = articles.filter(item => {
            if (!item.link) return false;
            if (lastCheckTime === 0) return true;
            const d = parseDateStrToObject(item.published);
            return !d || d.valueOf() >= lastCheckTime;
        });

        console.log(`  [NEW]   ${identifier}: ${newArticles.length} to process`);
        if (newArticles.length === 0) return;

        // -----------------------------------------------------------------------
        // BATCH DB LOOKUPS — 3 parallel queries regardless of article count
        // -----------------------------------------------------------------------
        const allNmIds = [...new Set(newArticles.flatMap(a => a.relatedIds.filter(id => id.startsWith('nm'))))];
        const allTtIds = [...new Set(newArticles.flatMap(a => a.relatedIds.filter(id => id.startsWith('tt'))))];
        const allLinks = newArticles.map(a => a.link);

        const [socialRes, mediaRes, existingRes] = await Promise.all([
            allNmIds.length
                ? supabase.from('hb_socials').select('identifier, linked_talent').in('identifier', allNmIds).not('linked_talent', 'is', null)
                : Promise.resolve({ data: [] as any[] }),
            allTtIds.length
                ? supabase.from('hb_media').select('id, soc_imdb_id').in('soc_imdb_id', allTtIds)
                : Promise.resolve({ data: [] as any[] }),
            supabase.from('news').select('id, source_link, tagged_talent, tagged_media, linked_talent_ids, linked_media_ids, internal_notes').in('source_link', allLinks),
        ]);

        // O(1) lookup maps
        const nmToUuid:    Record<string, string> = Object.fromEntries((socialRes.data  || []).map((s: any) => [s.identifier,  s.linked_talent]));
        const ttToUuid:    Record<string, string> = Object.fromEntries((mediaRes.data   || []).map((m: any) => [m.soc_imdb_id, m.id]));
        const existingMap: Record<string, any>    = Object.fromEntries((existingRes.data || []).map((e: any) => [e.source_link, e]));

        // -----------------------------------------------------------------------
        // Build insert / update batches — pure in-memory, zero DB calls
        // -----------------------------------------------------------------------
        const toInsert: any[] = [];
        const toUpdate: any[] = [];

        for (const item of newArticles) {
            const pubDateIso = parseDateStrToObject(item.published)?.toISOString() ?? null;

            // Raw IMDb IDs found in article text — stored regardless of whether
            // they exist in our DB yet, enabling backfill when new talent/media are added.
            const rawNmIds = item.relatedIds.filter(id => id.startsWith('nm'));
            const rawTtIds = item.relatedIds.filter(id => id.startsWith('tt'));

            const talentUuids = rawNmIds.map(id => nmToUuid[id]).filter(Boolean);
            const mediaUuids  = rawTtIds.map(id => ttToUuid[id]).filter(Boolean);

            const newTagsTalent = [...new Set([
                ...(profile.linked_talent ? [profile.linked_talent] : []),
                ...talentUuids,
            ])];
            const newTagsMedia = [...new Set(mediaUuids)];
            const newNotes     = item.imgNote ? [item.imgNote] : [];

            const existing = existingMap[item.link];

            if (existing) {
                const combinedTalent = [...new Set([...(existing.tagged_talent    || []), ...newTagsTalent])];
                const combinedMedia  = [...new Set([...(existing.tagged_media     || []), ...newTagsMedia])];
                const combinedNmIds  = [...new Set([...(existing.linked_talent_ids || []), ...rawNmIds])];
                const combinedTtIds  = [...new Set([...(existing.linked_media_ids  || []), ...rawTtIds])];
                const combinedNotes  = [...new Set([...(existing.internal_notes    || []), ...newNotes])];

                const changed =
                    combinedTalent.length !== (existing.tagged_talent    || []).length ||
                    combinedMedia.length  !== (existing.tagged_media     || []).length ||
                    combinedNmIds.length  !== (existing.linked_talent_ids || []).length ||
                    combinedTtIds.length  !== (existing.linked_media_ids  || []).length ||
                    combinedNotes.length  !== (existing.internal_notes    || []).length;

                if (changed) {
                    toUpdate.push({
                        id:                existing.id,
                        tagged_talent:     combinedTalent,
                        tagged_media:      combinedMedia,
                        linked_talent_ids: combinedNmIds,
                        linked_media_ids:  combinedTtIds,
                        internal_notes:    combinedNotes,
                    });
                }
            } else {
                toInsert.push({
                    article_title:     item.title,
                    article_heading:   null,
                    article:           item.text,
                    source_name:       item.source,
                    source_link:       item.link,
                    image_primary:     item.img,
                    published:         pubDateIso,
                    status:            'in progress',
                    public_visible:    true,
                    tagged_talent:     newTagsTalent,
                    tagged_media:      newTagsMedia,
                    linked_talent_ids: rawNmIds,
                    linked_media_ids:  rawTtIds,
                    internal_notes:    newNotes,
                });
            }
        }

        // Batch insert — one round-trip for all new articles
        if (toInsert.length > 0) {
            const { error: insertErr } = await supabase.from('news').insert(toInsert);
            if (insertErr) console.error(`  [ERR] ${identifier} batch insert:`, insertErr.message);
            else           console.log(`  [INS] ${identifier}: inserted ${toInsert.length} articles`);
        }

        // Merges must be per-row (each has different combined arrays)
        for (const upd of toUpdate) {
            const { error } = await supabase.from('news')
                .update({
                    tagged_talent:     upd.tagged_talent,
                    tagged_media:      upd.tagged_media,
                    linked_talent_ids: upd.linked_talent_ids,
                    linked_media_ids:  upd.linked_media_ids,
                    internal_notes:    upd.internal_notes,
                })
                .eq('id', upd.id);
            if (error) console.error(`  [ERR] ${identifier} update ${upd.id}:`, error.message);
        }
        if (toUpdate.length > 0) {
            console.log(`  [UPD] ${identifier}: merged tags on ${toUpdate.length} articles`);
        }

    } catch (e: any) {
        console.error(`[FAIL] ${identifier}:`, e.message);
        throw e; // re-throw so the caller can track failure count
    } finally {
        // Always stamp in finally — even on error — so a broken profile doesn't
        // sit permanently at the front of the queue blocking everything else.
        // Exception: 0 articles on a previously-checked profile suggests a scrape
        // failure (bot block, layout change) — don't advance the timestamp.
        const shouldStamp = articles.length > 0 || !profile.checked_imdb_news;
        if (shouldStamp) {
            await supabase.from('hb_socials')
                .update({ checked_imdb_news: new Date().toISOString() })
                .eq('id', profile.id);
            console.log(`[DONE] ${identifier} — timestamp updated`);
        } else {
            console.log(`[WARN] ${identifier} — 0 articles extracted, timestamp NOT updated (will retry)`);
        }
        await page.close();
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function scrapeNews(): Promise<void> {
    const startTime = Date.now();
    console.log('=== IMDb News Scraper ===');
    console.log(`Concurrency: ${CONCURRENCY} | Batch: ${PROFILE_LIMIT > 0 ? PROFILE_LIMIT : 'all'} profiles\n`);

    await logWorkflowRun('running');

    const { data: profiles, error } = await withRetry(() => supabase
        .from('hb_socials')
        .select('id, identifier, linked_talent, type, social_url, checked_imdb_news')
        .like('identifier', 'nm%')
        .order('checked_imdb_news', { ascending: true, nullsFirst: true })
        .limit(PROFILE_LIMIT > 0 ? PROFILE_LIMIT : 5000)); // Supabase defaults to 1000 without explicit limit

    if (error) {
        console.error('Error fetching profiles:', error);
        await logWorkflowRun('failure', 0, error.message);
        return;
    }
    console.log(`Fetched ${profiles.length} profiles.\n`);

    const browser: Browser = await puppeteer.launch({
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
    });

    const limit = createLimiter(CONCURRENCY);
    let successCount = 0;
    let failureCount = 0;

    await Promise.all((profiles as SocialProfile[]).map(profile =>
        limit(async () => {
            try {
                await scrapeProfile(browser, profile);
                successCount++;
            } catch (_) {
                failureCount++;
            }
        })
    ));

    await browser.close();

    const durationSecs = Math.round((Date.now() - startTime) / 1000);
    console.log(`\n=== Done in ${durationSecs}s (${successCount} ok, ${failureCount} failed) ===`);

    await logWorkflowRun(
        failureCount > 0 ? 'partial' : 'success',
        durationSecs,
        failureCount > 0 ? `${failureCount} profiles failed` : undefined
    );

    // Update success/failure counts on the workflow record
    if (WORKFLOW_ID) {
        await supabase.from('workflows')
            .update({
                success_count: supabase.rpc as any, // incremented via RPC in log_workflow_run
                last_run_duration_secs: durationSecs,
            })
            .eq('id', WORKFLOW_ID);
    }
}

scrapeNews().catch(async (e) => {
    console.error('Fatal error:', e);
    await logWorkflowRun('failure', 0, e.message);
    process.exit(1);
});
