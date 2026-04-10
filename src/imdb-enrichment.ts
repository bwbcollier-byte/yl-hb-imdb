import { supabase } from './supabase';
import * as countries from 'i18n-iso-countries';
import * as dotenv from 'dotenv';
dotenv.config();

// @ts-ignore
countries.registerLocale(require('i18n-iso-countries/langs/en.json'));

const RAPIDAPI_KEY  = process.env.RAPIDAPI_KEY!;
const RAPIDAPI_HOST = 'imdb236.p.rapidapi.com';
const LIMIT         = parseInt(process.env.LIMIT       || '100');
const STALE_DAYS    = parseInt(process.env.STALE_DAYS  || '30');
const CONCURRENCY   = parseInt(process.env.CONCURRENCY || '4');
const SLEEP_MS      = parseInt(process.env.SLEEP_MS    || '2500');
const WORKFLOW_ID   = parseInt(process.env.WORKFLOW_ID || '0');

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function chunk<T>(arr: T[], size: number): T[][] {
    const out: T[][] = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
}

// ─── COUNTRY CODE HELPER ─────────────────────────────────────────────────────

function getIsoCode(location: string | null): string | null {
    if (!location) return null;
    const parts = location.split(',').map(p => p.trim());
    const countryName = parts[parts.length - 1];
    // @ts-ignore
    let code = countries.getAlpha2Code(countryName, 'en');
    if (!code) {
        const overrides: Record<string, string> = {
            'USA': 'US', 'UK': 'GB', 'U.K.': 'GB', 'U.S.A.': 'US',
            'Scotland': 'GB', 'England': 'GB', 'Wales': 'GB',
            'Soviet Union': 'RU', 'Yugoslavia': 'RS', 'West Germany': 'DE',
        };
        code = overrides[countryName] || null;
    }
    return code || null;
}

// ─── IMDB API ────────────────────────────────────────────────────────────────

async function fetchImdbData(nmId: string, retries = 2): Promise<any | null> {
    try {
        const res = await fetch(`https://${RAPIDAPI_HOST}/api/imdb/name/${nmId}`, {
            headers: {
                'X-Rapidapi-Key':  RAPIDAPI_KEY,
                'X-Rapidapi-Host': RAPIDAPI_HOST,
                'accept': 'application/json',
            },
        });
        if (res.status === 429 && retries > 0) {
            console.log(`   ⏳ 429 Rate Limit for ${nmId}. Retrying in 10s...`);
            await sleep(10000);
            return fetchImdbData(nmId, retries - 1);
        }
        if (res.status === 404) return null;
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        return await res.json();
    } catch (err: any) {
        console.error(`   ❌ RapidAPI error for ${nmId}: ${err.message}`);
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
// Chunks the .in() query to avoid URL length limits (PostgREST breaks ~500+ UUIDs)

async function loadTalentMap(talentIds: string[]): Promise<Map<string, any>> {
    const map = new Map<string, any>();
    if (!talentIds.length) return map;

    for (const ids of chunk(talentIds, 200)) {
        const { data, error } = await supabase
            .from('hb_talent')
            .select('id, name, image, biography, birth_location, birth_country, date_birthdate, date_deathdate')
            .in('id', ids);
        if (error) { console.warn(`   ⚠️  Talent pre-load chunk error: ${error.message}`); continue; }
        for (const t of data || []) map.set(t.id, t);
    }
    return map;
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function run() {
    const runStart = Date.now();
    console.log(`🎬 IMDb Enrichment — up to ${LIMIT} records (stale > ${STALE_DAYS}d, concurrency: ${CONCURRENCY})\n`);

    const staleThreshold = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000).toISOString();

    // Only fetch valid person IDs (nm prefix) — skip tt (titles) and ch (characters)
    // Priority: never enriched first (detailed_array IS NULL), then stale
    const { data: socials, error } = await supabase
        .from('hb_socials')
        .select('id, identifier, name, linked_talent, check_imdb_enrichment')
        .eq('type', 'IMDB')
        .like('identifier', 'nm%')
        .or(`check_imdb_enrichment.is.null,check_imdb_enrichment.lt.${staleThreshold}`)
        .order('check_imdb_enrichment', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (error) throw error;
    if (!socials?.length) {
        console.log('✅ No stale IMDb profiles to enrich.');
        const durationSecs = Math.round((Date.now() - runStart) / 1000);
        await updateWorkflowSummary('success', { enriched: 0, run_at: new Date().toISOString() }, durationSecs);
        return;
    }

    // Prioritise records without any data at all (never enriched) over re-enriching old ones
    const neverEnriched = socials.filter(s => s.check_imdb_enrichment === null);
    const staleEnriched = socials.filter(s => s.check_imdb_enrichment !== null);
    const ordered = [...neverEnriched, ...staleEnriched].slice(0, LIMIT);

    console.log(`   Found ${ordered.length} profiles (${neverEnriched.length} never enriched, ${staleEnriched.length} stale).\n`);

    // Pre-load linked talent in chunked batches to avoid URL length limits
    const talentIds = [...new Set(ordered.map(s => s.linked_talent).filter(Boolean))];
    const talentMap = await loadTalentMap(talentIds);
    console.log(`   ✅ Pre-loaded ${talentMap.size} linked talent records\n`);

    // Pre-load valid country codes
    const { data: validCountryData } = await supabase.from('countries').select('country_code');
    const validCodes = new Set(validCountryData?.map(c => c.country_code) || []);

    const socialUpdates: any[] = [];
    const talentUpdates: any[] = [];
    let enrichedCount = 0, failedCount = 0;

    // Process in parallel batches of CONCURRENCY
    for (let i = 0; i < ordered.length; i += CONCURRENCY) {
        const batch = ordered.slice(i, i + CONCURRENCY);

        await Promise.all(batch.map(async (social) => {
            console.log(`🔍 ${social.name || social.identifier} (${social.identifier})`);
            const now = new Date().toISOString();

            const imdbData = await fetchImdbData(social.identifier);

            if (!imdbData) {
                // Stamp so we don't retry on every run
                socialUpdates.push({ id: social.id, check_imdb_enrichment: now });
                failedCount++;
                return;
            }

            socialUpdates.push({
                id:                    social.id,
                name:                  imdbData.name          || social.name,
                description:           imdbData.biography     || null,
                image:                 imdbData.primaryImage  || null,
                top_media:             imdbData.knownForTitles || null,
                detailed_array:        imdbData,
                check_imdb_enrichment: now,
                updated_at:            now,
            });
            enrichedCount++;
            console.log(`   ✅ ${imdbData.name || social.identifier}`);

            // Hydrate hb_talent — only fill fields that are currently blank
            if (social.linked_talent && talentMap.has(social.linked_talent)) {
                const t = talentMap.get(social.linked_talent)!;
                const tUpdate: any = { id: t.id };
                let changed = false;

                if (!t.image          && imdbData.primaryImage)  { tUpdate.image          = imdbData.primaryImage;  changed = true; }
                if (!t.biography      && imdbData.biography)     { tUpdate.biography      = imdbData.biography;     changed = true; }
                if (!t.birth_location && imdbData.birthLocation) { tUpdate.birth_location = imdbData.birthLocation; changed = true; }
                if (!t.date_birthdate && imdbData.birthDate)     { tUpdate.date_birthdate = imdbData.birthDate;     changed = true; }
                if (!t.date_deathdate && imdbData.deathDate)     { tUpdate.date_deathdate = imdbData.deathDate;     changed = true; }
                // Note: stats_height is numeric but IMDb returns a string (e.g. "5' 11\"") — skip to avoid type errors

                if (!t.birth_country && imdbData.birthLocation) {
                    const iso = getIsoCode(imdbData.birthLocation);
                    if (iso && validCodes.has(iso.toUpperCase())) {
                        tUpdate.birth_country = iso.toUpperCase();
                        changed = true;
                    }
                }

                if (changed) {
                    talentUpdates.push(tUpdate);
                }
            }
        }));

        await sleep(SLEEP_MS);
    }

    // ── Batch apply all updates in one round-trip each ───────────────────────
    console.log(`\n💾 Applying batch updates...`);

    if (socialUpdates.length > 0) {
        const { error: sErr } = await supabase.from('hb_socials').upsert(socialUpdates);
        if (sErr) console.error(`❌ Social batch error: ${sErr.message}`);
        else console.log(`   ✅ Updated ${socialUpdates.length} social records`);
    }

    if (talentUpdates.length > 0) {
        // Chunk talent upserts to avoid payload limits
        for (const ch of chunk(talentUpdates, 200)) {
            const { error: tErr } = await supabase.from('hb_talent').upsert(ch);
            if (tErr) console.error(`❌ Talent batch error: ${tErr.message}`);
        }
        console.log(`   ✅ Hydrated ${talentUpdates.length} talent profiles`);
    }

    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj = {
        profiles_processed: ordered.length,
        enriched:           enrichedCount,
        failed:             failedCount,
        talent_hydrated:    talentUpdates.length,
        run_at:             new Date().toISOString(),
    };
    console.log(`\n🎉 Done! ${enrichedCount} enriched, ${failedCount} failed, ${talentUpdates.length} talent hydrated (${durationSecs}s)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

run().catch(async (err) => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary('failure', { error: err.message, run_at: new Date().toISOString() }, 0);
    process.exit(1);
});
