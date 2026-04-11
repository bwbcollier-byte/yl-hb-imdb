import { supabase } from './supabase';
import * as dotenv from 'dotenv';
dotenv.config();

const RAPIDAPI_KEY  = process.env.RAPIDAPI_KEY!;
const RAPIDAPI_HOST = 'spotify-api25.p.rapidapi.com';
const LIMIT         = parseInt(process.env.LIMIT       || '100');
const STALE_DAYS    = parseInt(process.env.STALE_DAYS  || '30');
const CONCURRENCY   = parseInt(process.env.CONCURRENCY || '3');
const SLEEP_MS      = parseInt(process.env.SLEEP_MS    || '1500');
const WORKFLOW_ID   = parseInt(process.env.WORKFLOW_ID || '0');

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function chunk<T>(arr: T[], size: number): T[][] {
    const out: T[][] = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
}

function stripHtml(html: string | null | undefined): string | null {
    if (!html) return null;
    return html
        .replace(/<[^>]*>?/gm, '')
        .replace(/&amp;/g, '&')
        .replace(/&quot;/g, '"')
        .replace(/&apos;/g, "'")
        .trim() || null;
}

// ─── SPOTIFY API ─────────────────────────────────────────────────────────────

async function fetchSpotifyArtist(spotifyId: string, retries = 2): Promise<any | null> {
    try {
        const res = await fetch(`https://${RAPIDAPI_HOST}/getartist`, {
            method: 'POST',
            headers: {
                'x-rapidapi-key':  RAPIDAPI_KEY,
                'x-rapidapi-host': RAPIDAPI_HOST,
                'Content-Type':    'application/json',
            },
            body: JSON.stringify({ id: spotifyId }),
            signal: AbortSignal.timeout(20000),
        });

        if (res.status === 429 && retries > 0) {
            console.log(`   ⏳ 429 Rate Limit for ${spotifyId}. Retrying in 10s...`);
            await sleep(10000);
            return fetchSpotifyArtist(spotifyId, retries - 1);
        }
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);

        const json = await res.json();
        return json?.data?.artistUnion || null;
    } catch (err: any) {
        if (err.name === 'TimeoutError' && retries > 0) {
            console.log(`   ⏳ Timeout for ${spotifyId}. Retrying in 2s...`);
            await sleep(2000);
            return fetchSpotifyArtist(spotifyId, retries - 1);
        }
        console.error(`   ❌ Spotify API error for ${spotifyId}: ${err.message}`);
        return null;
    }
}

// ─── SUPABASE WORKFLOW SUMMARY ───────────────────────────────────────────────

async function updateWorkflowSummary(
    status: 'success' | 'failure',
    summary: Record<string, unknown>,
    durationSecs: number
) {
    if (!WORKFLOW_ID) return;
    const { error } = await supabase.rpc('log_workflow_run', {
        p_workflow_id:   WORKFLOW_ID,
        p_status:        status,
        p_duration_secs: durationSecs,
        p_summary:       summary,
    });
    if (error) console.warn(`   ⚠️  Workflow summary update failed: ${error.message}`);
    else console.log(`   📊 Workflow summary logged to Supabase`);
}

// ─── PRE-LOAD TALENT MAP ─────────────────────────────────────────────────────

async function loadTalentMap(talentIds: string[]): Promise<Map<string, any>> {
    const map = new Map<string, any>();
    if (!talentIds.length) return map;

    for (const ids of chunk(talentIds, 200)) {
        const { data, error } = await supabase
            .from('hb_talent')
            .select('id, name, image, biography')
            .in('id', ids);
        if (error) { console.warn(`   ⚠️  Talent pre-load chunk error: ${error.message}`); continue; }
        for (const t of data || []) map.set(t.id, t);
    }
    return map;
}

// ─── EXTERNAL SOCIAL IDENTIFIER EXTRACTION ───────────────────────────────────

function extractIdentifier(type: string, url: string): string | null {
    try {
        const parsed = new URL(url);
        const parts = parsed.pathname.split('/').filter(Boolean);
        if (type === 'INSTAGRAM' || type === 'FACEBOOK' || type === 'TWITTER') {
            return parts[0]?.split('?')[0] || null;
        }
        if (type === 'WIKIPEDIA') {
            return decodeURIComponent(parts[parts.length - 1]) || null;
        }
    } catch {}
    return null;
}

const EXTERNAL_SOCIAL_TYPES: Record<string, string> = {
    FACEBOOK:  'FACEBOOK',
    INSTAGRAM: 'INSTAGRAM',
    TWITTER:   'TWITTER',
    WIKIPEDIA: 'WIKIPEDIA',
};

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function run() {
    const runStart = Date.now();
    console.log(`🎵 Spotify Enrichment — up to ${LIMIT} records (stale > ${STALE_DAYS}d, concurrency: ${CONCURRENCY})\n`);

    const staleThreshold = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000).toISOString();

    // Priority: never enriched first (check_spotify_enrichment IS NULL), then stale
    const { data: socials, error } = await supabase
        .from('hb_socials')
        .select('id, identifier, name, linked_talent, check_spotify_enrichment')
        .eq('type', 'SPOTIFY')
        .or(`check_spotify_enrichment.is.null,check_spotify_enrichment.lt.${staleThreshold}`)
        .order('check_spotify_enrichment', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (error) throw error;
    if (!socials?.length) {
        console.log('✅ No stale Spotify profiles to enrich.');
        const durationSecs = Math.round((Date.now() - runStart) / 1000);
        await updateWorkflowSummary('success', { enriched: 0, run_at: new Date().toISOString() }, durationSecs);
        return;
    }

    const neverEnriched = socials.filter(s => s.check_spotify_enrichment === null);
    const staleEnriched = socials.filter(s => s.check_spotify_enrichment !== null);
    const ordered = [...neverEnriched, ...staleEnriched].slice(0, LIMIT);

    console.log(`   Found ${ordered.length} profiles (${neverEnriched.length} never enriched, ${staleEnriched.length} stale).\n`);

    // Pre-load linked talent in chunked batches
    const talentIds = [...new Set(ordered.map(s => s.linked_talent).filter(Boolean))];
    const talentMap = await loadTalentMap(talentIds);
    console.log(`   ✅ Pre-loaded ${talentMap.size} linked talent records\n`);

    const socialUpdates: any[]   = [];
    const talentUpdates: any[]   = [];
    const externalSocials: any[] = [];
    // Dedup keys: "linkedTalentId:type" — one external social per type per talent
    const externalSocialsSeen = new Set<string>();

    let enrichedCount = 0, failedCount = 0;

    for (let i = 0; i < ordered.length; i += CONCURRENCY) {
        const batch = ordered.slice(i, i + CONCURRENCY);

        await Promise.all(batch.map(async (social) => {
            console.log(`🔍 ${social.name || social.identifier} (${social.identifier})`);
            const now = new Date().toISOString();

            const artistData = await fetchSpotifyArtist(social.identifier);

            if (!artistData) {
                socialUpdates.push({ id: social.id, check_spotify_enrichment: now });
                failedCount++;
                return;
            }

            const profile  = artistData.profile;
            const visuals  = artistData.visuals;
            const stats    = artistData.stats;

            let rawBio = profile?.biography?.text;
            if (rawBio && typeof rawBio !== 'string') rawBio = String(rawBio);
            const cleanBio  = stripHtml(rawBio);
            const avatarUrl = visuals?.avatarImage?.sources?.[0]?.url || null;
            const headerUrl = visuals?.headerImage?.sources?.[0]?.url || null;
            const gallery   = (visuals?.gallery?.items || [])
                .map((item: any) => item.sources?.[0]?.url)
                .filter(Boolean);

            socialUpdates.push({
                id:                       social.id,
                name:                     profile?.name          || social.name,
                description:              cleanBio,
                image:                    avatarUrl,
                image_banner:             headerUrl,
                gallery_images:           gallery.length ? gallery : null,
                verified:                 profile?.verified      || false,
                followers:                stats?.followers       || 0,
                soc_sp_monthly_listeners: stats?.monthlyListeners || 0,
                popularity:               artistData.popularity  || 0,
                status:                   'Done',
                detailed_array:           artistData,
                check_spotify_enrichment: now,
                updated_at:               now,
            });
            enrichedCount++;
            console.log(`   ✅ ${profile?.name || social.identifier}`);

            // Hydrate hb_talent — only fill fields that are currently blank
            if (social.linked_talent && talentMap.has(social.linked_talent)) {
                const t = talentMap.get(social.linked_talent)!;
                const tUpdate: any = { id: t.id };
                let changed = false;

                if (!t.image     && avatarUrl) { tUpdate.image     = avatarUrl; changed = true; }
                if (!t.biography && cleanBio)  { tUpdate.biography = cleanBio;  changed = true; }

                if (changed) talentUpdates.push(tUpdate);
            }

            // Collect external socials discovered from Spotify profile links
            const externalLinks = artistData.profile?.externalLinks?.items || [];
            for (const link of externalLinks) {
                const type = EXTERNAL_SOCIAL_TYPES[link.name?.toUpperCase()];
                if (!type || !social.linked_talent) continue;

                const dedupeKey = `${social.linked_talent}:${type}`;
                if (externalSocialsSeen.has(dedupeKey)) continue;

                const identifier = extractIdentifier(type, link.url);
                if (!identifier) continue;

                externalSocialsSeen.add(dedupeKey);
                externalSocials.push({
                    type,
                    identifier,
                    handle:         identifier,
                    name:           profile?.name || social.name,
                    social_url:     link.url,
                    linked_talent:  social.linked_talent,
                    status:         'Done',
                    last_check:     now,
                });
            }
        }));

        await sleep(SLEEP_MS);
    }

    // ── Batch apply all updates ──────────────────────────────────────────────
    console.log(`\n💾 Applying batch updates...`);

    if (socialUpdates.length > 0) {
        const { error: sErr } = await supabase.from('hb_socials').upsert(socialUpdates);
        if (sErr) console.error(`❌ Social batch error: ${sErr.message}`);
        else console.log(`   ✅ Updated ${socialUpdates.length} social records`);
    }

    if (talentUpdates.length > 0) {
        for (const ch of chunk(talentUpdates, 200)) {
            const { error: tErr } = await supabase.from('hb_talent').upsert(ch);
            if (tErr) console.error(`❌ Talent batch error: ${tErr.message}`);
        }
        console.log(`   ✅ Hydrated ${talentUpdates.length} talent profiles`);
    }

    if (externalSocials.length > 0) {
        // Insert new external socials; skip rows that already exist (same type+identifier)
        const { error: eErr } = await supabase
            .from('hb_socials')
            .upsert(externalSocials, { onConflict: 'type,identifier', ignoreDuplicates: true });
        if (eErr) {
            // Non-fatal — fall back to individual inserts if bulk fails
            console.warn(`   ⚠️  Bulk external social upsert failed (${eErr.message}), skipping`);
        } else {
            console.log(`   ✅ Synced ${externalSocials.length} external socials (IG/TW/FB/WP)`);
        }
    }

    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj = {
        profiles_processed: ordered.length,
        enriched:           enrichedCount,
        failed:             failedCount,
        talent_hydrated:    talentUpdates.length,
        external_socials:   externalSocials.length,
        run_at:             new Date().toISOString(),
    };
    console.log(`\n🎉 Done! ${enrichedCount} enriched, ${failedCount} failed, ${talentUpdates.length} talent hydrated, ${externalSocials.length} external socials (${durationSecs}s)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

run().catch(async (err) => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary('failure', { error: err.message, run_at: new Date().toISOString() }, 0);
    process.exit(1);
});
