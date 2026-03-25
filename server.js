const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

// =============================
// CONFIG
// =============================
const MAX_SUBJECT_RETRIES = 3;
const PROCESS_INTERVAL_MS = 500;  // Check queue twice per second
const CONCURRENCY = 10;           // Safe for Render — domain cache reduces scrape load

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// =============================
// DOMAIN SCRAPE CACHE
// Scrape each domain once per server session, reuse for all contacts at that domain
// =============================
const domainCache = new Map(); // domain -> { newsBlock, blogBlock, cachedAt }
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour TTL

// Domains known to block scrapers — skip immediately
const BLOCKED_DOMAINS = new Set([
  'bankofamerica.com', 'wellsfargo.com', 'citigroup.com', 'citi.com',
  'chase.com', 'jpmorgan.com', 'goldmansachs.com', 'morganstanley.com',
  'herbalife.com', 'securityfinance.com', 'braze.com', 'salesforce.com',
  'microsoft.com', 'google.com', 'amazon.com', 'apple.com', 'meta.com',
  'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com'
]);

// =============================
// SMB HUBSPOT SERVICE LIBRARY
// Audience: 10-500 employee companies ALREADY USING HubSpot
// Titles: VP/Director of Marketing, Demand Gen, RevOps, Marketing Ops, Sales leadership
// Signal: Active HubSpot investment (recent hiring in Marketing/Sales/RevOps roles)
// Pain: Not "should you use HubSpot" but "are you getting full value from HubSpot"
// =============================
const TPG_HUBSPOT_SERVICES = [
  {
    label: "HubSpot CRM Optimization",
    url: "https://www.pedowitzgroup.com/hubspot-crm",
    smb_pain: "Most companies using HubSpot are only scratching the surface of the CRM. Pipeline visibility is murky, reps log activity inconsistently, and reporting doesn't tell the full story. We fix the foundation.",
    angle: "CRM optimization and pipeline visibility"
  },
  {
    label: "HubSpot Demand Generation",
    url: "https://www.pedowitzgroup.com/hubspot-demand-generation",
    smb_pain: "Having HubSpot and generating real pipeline from it are two different things. We build multi-channel demand programs inside HubSpot that convert, not just campaigns that look busy.",
    angle: "pipeline generation and lead conversion"
  },
  {
    label: "HubSpot Marketing Automation & Workflows",
    url: "https://www.pedowitzgroup.com/automate-repetitive-tasks-that-waste-10-hours-weekly-using-hubspot",
    smb_pain: "Growing teams with HubSpot often hit a wall. Workflows break, leads fall through cracks, and the automation that was supposed to save time starts creating more work. We rebuild it right.",
    angle: "workflow automation and operational efficiency"
  },
  {
    label: "HubSpot Sales Enablement",
    url: "https://www.pedowitzgroup.com/hubspot-sales-enablement",
    smb_pain: "HubSpot has powerful sales tools that most teams never fully activate. Sequences, playbooks, deal scoring, and coaching insights. We set them up so your reps close faster with less friction.",
    angle: "sales productivity and deal velocity"
  },
  {
    label: "HubSpot SEO & Organic Growth",
    url: "https://www.pedowitzgroup.com/increase-organic-traffic-200-with-hubspot-seo",
    smb_pain: "HubSpot's SEO and content tools are underused by most teams. We use them to build a topic cluster strategy that compounds organic pipeline quarter over quarter, without increasing headcount.",
    angle: "organic growth and content strategy"
  },
  {
    label: "HubSpot Platform Migration (Move It)",
    url: "https://www.pedowitzgroup.com/hubspot-move-it",
    smb_pain: "If your team is straddling HubSpot and a legacy MAP like Pardot or Marketo, you're paying for two systems and getting half the results from both. We've done 1,000+ migrations. Clean, fast, zero data loss.",
    angle: "platform consolidation and MarTech simplification"
  },
  {
    label: "HubSpot Loop Marketing Framework",
    url: "https://www.pedowitzgroup.com/what-is-hubspots-loop-marketing-framework",
    smb_pain: "HubSpot's Loop framework is designed to compound results each quarter. Most teams haven't operationalized it. We help you build the system, not just run campaigns.",
    angle: "modern marketing strategy and HubSpot Loop"
  },
  {
    label: "HubSpot Commerce Hub & Quote-to-Payment",
    url: "https://www.pedowitzgroup.com/reduce-time-from-quote-to-payment-by-50-using-hubspot-commerce-hub",
    smb_pain: "If your quoting, billing, and renewals still live outside HubSpot, you're missing revenue and creating reconciliation headaches. Commerce Hub fixes that and we set it up fast.",
    angle: "revenue operations and billing automation"
  },
  {
    label: "HubSpot Managed Services",
    url: "https://www.pedowitzgroup.com/solutions/martech/hubSpot",
    smb_pain: "As you scale and add headcount in Marketing and RevOps, the question isn't just who owns HubSpot. It's who's optimizing it. TPG acts as your on-call HubSpot team for strategy, execution, and continuous improvement.",
    angle: "ongoing HubSpot management and optimization"
  },
  {
    label: "HubSpot Email Deliverability & Engagement",
    url: "https://www.pedowitzgroup.com/reduce-unsubscribes-boost-engagement-in-hubspot",
    smb_pain: "Growing contact lists in HubSpot without a tight segmentation and cadence strategy leads to deliverability decay and rising unsubscribes. We rebuild the email program from the ground up.",
    angle: "email performance and list health"
  },
];

// =============================
// QUEUE, CONCURRENCY & ERROR TRACKING
// =============================
let queue = [];
let inFlight = 0; // Number of jobs currently being processed
let errorCount = 0; // Increments on permanent failure, resets on restart

// =============================
// HEALTH CHECK
// =============================
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    queueLength: queue.length,
    inFlight: inFlight,
    concurrency: CONCURRENCY,
    errorCount
  });
});

// =============================
// LIVE DASHBOARD
// =============================
app.get("/dashboard", (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>SMB HubSpot Nurture — Queue Monitor</title>
      <meta http-equiv="refresh" content="5">
      <style>
        body { font-family: monospace; background: #0f0f0f; color: #a2cf23; padding: 40px; }
        h1 { font-size: 18px; margin-bottom: 30px; color: #fff; }
        .grid { display: flex; gap: 60px; margin-bottom: 40px; }
        .block {}
        .stat { font-size: 64px; font-weight: bold; margin: 0; line-height: 1; }
        .label { font-size: 13px; color: #555; margin-top: 8px; }
        .green { color: #a2cf23; }
        .orange { color: #f0a500; }
        .red { color: #e05252; }
        .grey { color: #333; }
        .footer { font-size: 12px; color: #333; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
      </style>
    </head>
    <body>
      <h1>SMB HubSpot Nurture &mdash; Queue Monitor</h1>

      <div class="grid">
        <div class="block">
          <div class="stat ${queue.length > 0 ? 'orange' : 'grey'}">${queue.length}</div>
          <div class="label">contacts waiting in queue</div>
        </div>
        <div class="block">
          <div class="stat green">${inFlight}</div>
          <div class="label">in-flight (of ${CONCURRENCY} max slots)</div>
        </div>
        <div class="block">
          <div class="stat green">${CONCURRENCY - inFlight}</div>
          <div class="label">open slots available</div>
        </div>
        <div class="block">
          <div class="stat ${errorCount > 0 ? 'red' : 'grey'}">${errorCount}</div>
          <div class="label">processing errors (since last restart)</div>
        </div>
      </div>

      <div class="footer">
        Last refreshed: ${new Date().toLocaleTimeString()} &nbsp;·&nbsp; Auto-refreshes every 5 seconds
      </div>
    </body>
    </html>
  `);
});

// =============================
// ENQUEUE FROM HUBSPOT
// =============================
app.post("/enqueue", (req, res) => {
  queue.push({ ...req.body, retries: 0 });
  res.status(200).json({
    status: "queued",
    queuePosition: queue.length
  });
});

// =============================
// PROCESS A SINGLE JOB
// =============================
async function processJob(job) {
  try {
    await updateStatus(job.contactId, "IN_PROGRESS");

    const result = await runClaude(job);

    await writeResults(job.contactId, result, job.sequenceStep || 1);

    await updateStatus(job.contactId, "SENT");

    console.log(`✅ Completed: ${job.contactId} - Step ${job.sequenceStep}`);
  } catch (err) {
    console.error(`❌ Error for ${job.contactId}:`, err.message);

    if (err.response?.status === 429) {
      console.log(`⏳ Rate limited, requeuing ${job.contactId}`);
      queue.push(job);
    } else {
      job.retries = (job.retries || 0) + 1;

      if (job.retries <= 2) {
        await updateStatus(job.contactId, "RETRY_PENDING");
        queue.push(job);
      } else {
        errorCount++;
        await updateStatus(job.contactId, "FAILED");
      }
    }
  } finally {
    inFlight--;
  }
}

// =============================
// WORKER LOOP — CONCURRENT BATCH
// =============================
setInterval(() => {
  while (inFlight < CONCURRENCY && queue.length > 0) {
    const job = queue.shift();
    inFlight++;
    processJob(job);
  }

  if (queue.length > 0 || inFlight > 0) {
    console.log(`📊 Queue: ${queue.length} | In-flight: ${inFlight}`);
  }
}, PROCESS_INTERVAL_MS);

// =============================
// URL NORMALIZER
// =============================
function normalizeUrl(rawUrl) {
  if (!rawUrl) return null;
  let url = rawUrl.trim();
  url = url.replace(/\/+$/, '');
  if (!/^https?:\/\//i.test(url)) {
    url = 'https://' + url;
  }
  try {
    new URL(url);
    return url;
  } catch {
    return null;
  }
}

// =============================
// HTML STRIPPER
// =============================
function stripHtml(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// =============================
// TITLE EXTRACTION
// =============================
async function extractTitles(url) {
  const normalized = normalizeUrl(url);
  if (!normalized) return [];

  try {
    const res = await axios.get(normalized, {
      timeout: 4000,
      maxContentLength: 300000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    });

    const html = res.data || '';
    const headlineMatches = [];
    const headingRegex = /<h[12][^>]*>([\s\S]*?)<\/h[12]>/gi;
    let match;
    while ((match = headingRegex.exec(html)) !== null) {
      const text = match[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
      if (text.length >= 20 && text.length <= 160) {
        headlineMatches.push(text);
      }
    }
    if (headlineMatches.length >= 2) return headlineMatches.slice(0, 3);

    const text = stripHtml(html);
    const fallbackMatches = [...text.matchAll(/(.{25,120})\s+(20\d{2})/g)];
    return fallbackMatches.map(m => m[1].trim()).slice(0, 3);

  } catch (err) {
    const reason = err.code === 'ECONNABORTED' ? 'timeout'
      : err.response ? `HTTP ${err.response.status}`
      : err.message;
    console.log(`⚠️ Could not fetch ${normalized}: ${reason}`);
    return [];
  }
}

// =============================
// COMPANY RESEARCH — WITH DOMAIN CACHE
// =============================
async function getCompanyContent(website) {
  const baseUrl = normalizeUrl(website);
  if (!baseUrl) return { newsBlock: null, blogBlock: null };

  let domain = '';
  try {
    domain = new URL(baseUrl).hostname.replace(/^www\./, '');
  } catch {
    return { newsBlock: null, blogBlock: null };
  }

  if (BLOCKED_DOMAINS.has(domain)) {
    console.log(`🚫 Skipping blocked domain: ${domain}`);
    return { newsBlock: null, blogBlock: null };
  }

  if (domainCache.has(domain)) {
    const cached = domainCache.get(domain);
    if (Date.now() - cached.cachedAt < CACHE_TTL_MS) {
      console.log(`💾 Cache hit: ${domain}`);
      return { newsBlock: cached.newsBlock, blogBlock: cached.blogBlock };
    }
  }

  const newsPaths = [
    '/news', '/press', '/newsroom', '/press-releases',
    '/company-news', '/about/news', '/awards', '/recognition'
  ];
  const blogPaths = [
    '/blog', '/insights', '/resources', '/thought-leadership', '/articles'
  ];

  let newsBlock = null;
  let blogBlock = null;

  for (const path of newsPaths) {
    const titles = await extractTitles(`${baseUrl}${path}`);
    if (titles.length >= 1) {
      newsBlock = `COMPANY NEWS & AWARDS (VERIFIED from ${baseUrl}${path}):\n`
        + titles.map(t => `- ${t}`).join('\n');
      console.log(`📰 Found news at ${baseUrl}${path}`);
      break;
    }
  }

  for (const path of blogPaths) {
    const titles = await extractTitles(`${baseUrl}${path}`);
    if (titles.length >= 1) {
      blogBlock = `COMPANY BLOGS & PRESS (VERIFIED from ${baseUrl}${path}):\n`
        + titles.map(t => `- ${t}`).join('\n');
      console.log(`📝 Found blog at ${baseUrl}${path}`);
      break;
    }
  }

  domainCache.set(domain, { newsBlock, blogBlock, cachedAt: Date.now() });
  console.log(`💾 Cached: ${domain} (${domainCache.size} domains in cache)`);

  return { newsBlock, blogBlock };
}

// =============================
// AUDIENCE PROFILE CLASSIFIER
// Based on Apollo criteria: 10-500 employees, already using HubSpot,
// recent hiring in Marketing/Sales/RevOps, VP/Director-level titles
// =============================
function classifySmbProfile(numemployees) {
  const emp = parseInt(numemployees) || 0;

  if (emp <= 50) {
    return {
      tier: "small business",
      context: "a small but growing team (10–50 people) where the marketing leader is hands-on and likely wearing multiple hats alongside managing HubSpot",
      hubspot_angle: "Has HubSpot but probably hasn't had time to optimize it. Workflows are basic, reporting is underbuilt, and automation isn't fully activated",
      hiring_signal: "Recent Marketing/Sales/RevOps hires signal they're investing in the function and likely feeling the strain of scaling with their current HubSpot setup"
    };
  } else if (emp <= 200) {
    return {
      tier: "growing mid-SMB",
      context: "a mid-sized company (50–200 people) with a real marketing team that's actively scaling. VP or Director level is accountable for pipeline and HubSpot ROI",
      hubspot_angle: "Using HubSpot Pro or Enterprise, but likely has gaps in sales and marketing alignment, attribution reporting, and automation sophistication",
      hiring_signal: "Active RevOps and Demand Gen hiring signals they're trying to get more out of HubSpot and need a partner who can accelerate that"
    };
  } else {
    return {
      tier: "established SMB",
      context: "a well-established company (200–500 people) with a mature marketing function. Leadership is focused on HubSpot ROI, pipeline predictability, and scaling without adding headcount",
      hubspot_angle: "Likely on HubSpot Enterprise. Strategic priorities are optimization, advanced automation, and tying HubSpot data to revenue outcomes",
      hiring_signal: "Marketing Ops and RevOps hiring at this size means they're serious about the platform and likely evaluating whether to build in-house or partner with an expert"
    };
  }
}

// =============================
// DASH REMOVER — POST-PROCESS SAFETY NET
// =============================
function removeDashes(text) {
  return text
    .replace(/\s*—\s*/g, ', ')
    .replace(/\s*–\s*/g, ', ')
    .replace(/  +/g, ' ')
    .trim();
}

// =============================
// CLAUDE LOGIC
// =============================
async function runClaude(job) {
  const SEQUENCE_STEP = job.sequenceStep || 1;

  const {
    firstname = '',
    company = '',
    jobtitle = '',
    industry = '',
    numemployees = '',
    annualrevenue = '',
    hs_linkedin_url = '',
    website = '',
    hs_intent_signals_enabled = '',
    web_technologies = '',
    description = '',
    hs_analytics_last_url = '',
    hs_analytics_num_page_views = ''
  } = job;

  const IntentContext =
    hs_intent_signals_enabled === "true"
      ? "Buyer intent signals are active for this account."
      : "Buyer intent signals are not active or unavailable.";

  const smbProfile = classifySmbProfile(numemployees);

  // =============================
  // BEHAVIORAL SIGNALS ANALYSIS
  // =============================
  let behavioralContext = '';
  const pageViews = parseInt(hs_analytics_num_page_views) || 0;
  const lastUrl = hs_analytics_last_url ? hs_analytics_last_url.trim() : '';

  if (pageViews >= 10) {
    behavioralContext += `High website engagement (${pageViews} pages viewed) - showing strong active interest. `;
  } else if (pageViews >= 5) {
    behavioralContext += `Moderate website engagement (${pageViews} pages) - exploring solutions. `;
  } else if (pageViews >= 1) {
    behavioralContext += `Initial website visit (${pageViews} pages) - early awareness stage. `;
  } else {
    behavioralContext += `No prior website visits detected. `;
  }

  if (lastUrl) {
    let topicInterest = '';
    const url = lastUrl.toLowerCase();

    if (url.includes('/pricing')) {
      topicInterest = 'Viewed pricing - evaluating investment';
    } else if (url.includes('/demo') || url.includes('/get-started')) {
      topicInterest = 'Visited demo/get-started page - high intent';
    } else if (url.includes('/case-stud') || url.includes('/customer')) {
      topicInterest = 'Reviewed case studies - seeking proof points';
    } else if (url.includes('/integration') || url.includes('/connect')) {
      topicInterest = 'Explored integrations - technical evaluation';
    } else if (url.includes('/blog') || url.includes('/resource')) {
      topicInterest = 'Consumed content - educational phase';
    } else if (url.includes('/hubspot')) {
      topicInterest = 'Specifically researched HubSpot solutions';
    } else if (url.includes('/revenue') || url.includes('/marketing')) {
      topicInterest = 'Focused on revenue marketing solutions';
    } else {
      const pageName = lastUrl.split('/').filter(p => p).pop()?.replace(/-/g, ' ') || 'homepage';
      topicInterest = `Last viewed: ${pageName}`;
    }

    if (topicInterest) {
      behavioralContext += topicInterest + '.';
    }
  }

  const BehavioralContext = behavioralContext.trim() || 'No behavioral data available.';

  const serviceIndex = (SEQUENCE_STEP - 1) % TPG_HUBSPOT_SERVICES.length;
  const featuredService = TPG_HUBSPOT_SERVICES[serviceIndex];

  // =============================
  // PRIOR EMAILS
  // =============================
  let priorEmailsText = [];
  for (let i = 1; i < SEQUENCE_STEP; i++) {
    const field = job[`tpg_smb_hubspot_nurture_claude_text_em${i}`];
    if (field) priorEmailsText.push(`EMAIL ${i}:\n${field}`);
  }

  const priorEmailsBlock = priorEmailsText.length
    ? priorEmailsText.join("\n\n---\n\n")
    : "N/A";

  // =============================
  // NEWS & BLOG EXTRACTION
  // =============================
  const defaultNewsBlock = `COMPANY NEWS & AWARDS (VERIFIED):\n- None found`;
  const defaultBlogBlock = `COMPANY BLOGS & PRESS (VERIFIED):\n- None found`;

  let companyNewsBlock = defaultNewsBlock;
  let companyContentBlock = defaultBlogBlock;

  if (website) {
    try {
      const { newsBlock, blogBlock } = await getCompanyContent(website);
      if (newsBlock) companyNewsBlock = newsBlock;
      if (blogBlock) companyContentBlock = blogBlock;
    } catch (err) {
      console.log(`⚠️ Content extraction failed for ${company}: ${err.message}`);
    }
  }

  const userContent = `You are Jeff Pedowitz, Founder and CEO of The Pedowitz Group (TPG), writing EMAIL ${SEQUENCE_STEP} in a 10-touch personalized outbound nurture sequence targeting SMB prospects.

YOUR MISSION FOR THIS SEQUENCE:
Get this prospect to give TPG their HubSpot business. Every email must make a compelling case for why TPG is the right HubSpot partner for a company their size that's actively investing in the platform.

CRITICAL AUDIENCE CONTEXT:
- These contacts are ALREADY USING HUBSPOT. Do NOT write as if they need to be convinced to use HubSpot.
- They are VP or Director-level in Marketing, Demand Gen, RevOps, Marketing Ops, or Sales at a 10–500 person company.
- They have recently hired for Marketing, Sales, or RevOps roles, a strong signal of active HubSpot investment and scaling pressure.
- The pitch is: "You're investing in HubSpot. TPG makes sure that investment pays off."
- They are NOT looking for a reason to use HubSpot. They ARE looking for a reason to bring in an expert partner vs. figuring it out themselves.

ABOUT THE PEDOWITZ GROUP:
TPG is a certified HubSpot partner and Revenue Marketing consultancy. We specialize in helping SMBs get real value from HubSpot, from CRM optimization and automation, to demand generation, sales enablement, SEO, platform migration, and fully managed HubSpot services. TPG has worked with 1,300+ clients and generated over $25B in marketing-sourced revenue. Our clients have won 50+ national Revenue Marketing awards.

FEATURED HUBSPOT SERVICE FOR THIS EMAIL:
- Service: ${featuredService.label}
- URL: ${featuredService.url}
- Pain Point: ${featuredService.smb_pain}
- Angle: ${featuredService.angle}

PROSPECT DATA:
- Name: ${firstname}
- Title: ${jobtitle}
- Company: ${company}
- Industry: ${industry}
- Employee Count: ${numemployees}
- Annual Revenue: ${annualrevenue}
- LinkedIn: ${hs_linkedin_url}
- Website: ${website}
- Intent Signals: ${IntentContext}
- Web Technologies: ${web_technologies || "Not listed"}
- Company Description: ${description || "Not provided"}

AUDIENCE PROFILE:
- Tier: ${smbProfile.tier}
- Context: ${smbProfile.context}
- HubSpot Situation: ${smbProfile.hubspot_angle}
- Hiring Signal: ${smbProfile.hiring_signal}

BEHAVIORAL SIGNALS (WEBSITE ACTIVITY):
${BehavioralContext}

${companyNewsBlock}
${companyContentBlock}

PRIOR EMAILS — BACKGROUND CONTEXT ONLY:
Everything below has ALREADY been sent to this contact.
${priorEmailsBlock}

ABSOLUTE NON-REPETITION RULES (HARD FAIL CONDITIONS):
- You MUST NOT repeat any idea, insight, pain point, example, framing, or analogy used in ANY prior email.
- You MUST NOT reuse sentence structure, paragraph structure, or opening style from prior emails.
- You MUST introduce a NEW HubSpot service angle or challenge that advances the conversation.
- If similarity to ANY prior email exceeds a minimal level, the response is INVALID.

SUBJECT LINE NON-REPETITION REQUIREMENTS (HARD RULE):
- The subject line MUST be entirely unique and clearly distinct from all prior subject lines.
- You MUST NOT reuse, closely paraphrase, or slightly modify previous subject lines.
- If the subject line is semantically or structurally similar to any prior subject, the response is INVALID.

NEWS & AWARDS USAGE RULE (PRIORITIZED):
- If the "COMPANY NEWS & AWARDS (VERIFIED)" section contains items:
  - You MUST reference ONE of them in your opening personalization. This is your strongest hook.
  - Reference it conversationally: "I saw that [Company] recently..." or "With [Company] rolling out..."

BLOG / PRESS USAGE RULE (PRIORITIZED):
- If the "COMPANY BLOGS & PRESS (VERIFIED)" section contains items:
  - You SHOULD reference ONE by topic/theme to show you understand their content strategy.

SMB-SPECIFIC TONE & MESSAGING RULES:
- Write peer-to-peer: Jeff is talking to a VP or Director who owns HubSpot outcomes at their company.
- They already bought HubSpot. The question is whether they're getting full value from it.
- Acknowledge the reality of scaling with HubSpot: new hires to onboard, workflows that need rebuilding, reporting that doesn't quite tell the full story yet.
- The hiring signal is a POWERFUL hook. They're actively building out Marketing/RevOps, which means HubSpot complexity is growing and so is the pressure to perform.
- NEVER say "you should try HubSpot" or "have you considered HubSpot" — they're already on it.
- NEVER write as if you're pitching to a Fortune 500 CMO — this is a hands-on VP or Director at a growing company.
- Avoid enterprise buzzwords: "digital transformation," "omnichannel orchestration," "scalable MarTech ecosystem."
- Use direct, confident language: "you're probably hitting the ceiling on what basic workflows can do," "as your team grows, HubSpot complexity grows with it," "most teams at your stage aren't getting full ROI from their HubSpot investment."
- Tone: direct, credible, peer-level, like a trusted advisor who's solved this exact problem 1,300 times.

WRITE EMAIL ${SEQUENCE_STEP} WITH THESE REQUIREMENTS:

WRITE:
- Subject: 8 words or fewer and DIFFERENT from all prior subjects.
- Start with a salutation on its own line:
  "${firstname},"
- One blank line after salutation.
- Opening line MUST be highly specific and personalized. Lead with a concrete observation about the prospect's company, recent news, or their specific situation.
- Body length: 75–100 words. Keep it tight. These are busy directors, not blog readers.
- Each paragraph separated by ONE blank line.
- No bullets. No signature.
- Return HTML-safe text.
- Use <a> tags only for links. No other HTML.
- NEVER use dashes, hyphens, or em dashes (— or – or -) anywhere in the subject line or body.
- Connect ideas with commas, conjunctions, or new sentences instead of dashes.
- BAD: "workflows break, leads fall through cracks — and automation creates more work" GOOD: "workflows break, leads fall through cracks, and automation starts creating more work"
- No throat-clearing openers. Get straight to the point in the first sentence.

MESSAGING STRATEGY:
- Each email spotlights ONE HubSpot use case as the primary angle.
- For this email, lead with "${featuredService.label}" as your primary hook.
- Rotate pain point focus across the sequence: HubSpot adoption, pipeline visibility, automation gaps, content/SEO, sales and marketing alignment, platform migration, billing/revenue ops, managed services, email performance, and reporting.
- Position TPG as the HubSpot partner that gets results fast, not a consultant who delivers a 90-page strategy deck.
- Make every email feel like it was written specifically for THIS company and THIS person.

PERSONALIZATION & 1:1 OUTREACH REQUIREMENTS (MANDATORY):
- The email must feel like Jeff typed it himself in 3 minutes. Not polished. Not structured. Just direct and real.
- OPENING REQUIREMENT: The first sentence after the greeting MUST contain highly specific, concrete personalization.
- BEHAVIORAL SIGNALS USAGE: If the prospect has website activity, weave this in naturally.
- CONVERSATIONAL TONE: Write as if you've been following their company. Use phrases like:
  * "I noticed you recently..."
  * "With [Company] growing fast in [industry]..."
  * "Given that [Company] is actively building out your RevOps function..."
  * "I saw that [Company]..."
- GOOD EXAMPLES (appropriate for this audience):
  * "I saw [Company] has been hiring for Marketing Ops and Demand Gen lately. That kind of growth usually means your HubSpot setup is working overtime and could use some expert tuning."
  * "With a team scaling the way [Company] is, I'd bet your HubSpot workflows are starting to show their limits. Routing gets messy, reporting gets inconsistent, and the automation you built six months ago doesn't quite fit anymore."
  * "I noticed you visited our page on HubSpot demand generation. Sounds like you're thinking about how to get more pipeline out of the platform you've already invested in."
  * "Given that [Company] is actively building out your RevOps function, now is exactly the right time to make sure your HubSpot foundation can scale with the team."
- BAD EXAMPLES (NEVER DO THIS):
  * "Have you considered using HubSpot?" (they're already on it)
  * "As a leader in the [industry] space..." (too generic)
  * "Many companies like yours are facing..." (not specific)
  * "In today's competitive landscape..." (templated filler)
  * "I hope this message finds you well..." (filler opener)
- SPECIFICITY TEST: If you could swap the company name and send this to 5 other companies, the personalization has FAILED.

LINKED CONTENT REQUIREMENTS:
- Include ONE paragraph with exactly ONE single-word hyperlink pointing to the featured service URL:
  <a href="${featuredService.url}" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">word</a>
- Include ONE separate paragraph with a calendar CTA using:
  <a href="https://meetings.hubspot.com/jeff-pedowitz" style="font-weight:bold;text-decoration:underline;color:#A2CF23;">word</a>

COMPLIANCE:
- No dollar amounts unless public.
- No fabricated company news.
- Speak to industry patterns when specifics are unknown.

OUTPUT FORMAT (exactly):
Subject: <subject>

Body:
<body>`;

  let subject = "";
  let bodyText = "";
  let attempt = 0;

  while (attempt < MAX_SUBJECT_RETRIES && !subject) {
    attempt++;

    const res = await axios.post(
      "https://api.anthropic.com/v1/messages",
      {
        model: "claude-sonnet-4-20250514",
        max_tokens: 1500,
        temperature: 0.7,
        system: `You are Jeff Pedowitz, founder of The Pedowitz Group, writing a quick personal email to a prospect. Write the way a busy executive actually types: short sentences, natural rhythm, occasional fragments are fine. No corporate polish, no perfect parallelism. Think "typed this between meetings" not "crafted by a marketer."
Your audience is VP and Director-level Marketing, Demand Gen, RevOps, and Sales leaders at 10–500 person companies that are ALREADY USING HUBSPOT and actively hiring in the function.
Every email must:
1. Open with something specific about THIS company, their hiring activity, recent news, or their HubSpot growth stage. Never a generic opener.
2. Assume they're already on HubSpot. The pitch is about getting MORE value from it, not convincing them to adopt it.
3. Spotlight ONE specific HubSpot capability or TPG service that solves a real problem at their stage of growth.
4. Stay under 100 words in the body. These are busy directors, not blog readers.
5. Sound like a peer-to-peer note from a HubSpot expert, not a sales blast.
6. Use the recent hiring signal as a natural hook when relevant. It's a strong indicator they're scaling HubSpot complexity.
Each email in the sequence must introduce a fresh HubSpot angle, never repeating a pain point, hook, or CTA from a prior email.`,
        messages: [{ role: "user", content: userContent }]
      },
      {
        headers: {
          "x-api-key": ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
          "content-type": "application/json"
        },
        timeout: 30000
      }
    );

    const text =
      res.data?.content?.find(p => p.type === "text")?.text || "";

    const subjectMatch = text.match(/^\s*Subject:\s*(.+)\s*$/mi);
    const bodyMatch =
      text.match(/^\s*Body:\s*([\s\S]+)$/mi) ||
      text.match(/^\s*Subject:[\s\S]*?\n\n([\s\S]+)$/mi);

    subject = subjectMatch ? subjectMatch[1].trim().replace(/<[^>]+>/g, '') : "";
    bodyText = bodyMatch ? bodyMatch[1].trim() : "";
  }

  if (!subject) {
    throw new Error("Missing subject after retries");
  }

  return { subject: removeDashes(subject), bodyText: removeDashes(bodyText) };
}

// =============================
// HUBSPOT WRITE-BACK
// Properties: tpg_smb_hubspot_nurture_subject_line_em1-10
//             tpg_smb_hubspot_nurture_em1-10
//             tpg_smb_hubspot_nurture_claude_text_em1-10
// =============================
async function writeResults(contactId, { subject, bodyText }, sequenceStep = 1) {
  const bodyHtml = bodyText
    .replace(/\r\n/g, "\n")
    .split(/\n{2,}/)
    .map(p => `<p style="margin:0 0 16px;">${p.replace(/\n/g, "<br>")}</p>`)
    .join("\n");

  await axios.patch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
    {
      properties: {
        [`tpg_smb_hubspot_nurture_subject_line_em${sequenceStep}`]: subject,
        [`tpg_smb_hubspot_nurture_em${sequenceStep}`]: bodyHtml,
        [`tpg_smb_hubspot_nurture_claude_text_em${sequenceStep}`]: bodyText
      }
    },
    {
      headers: {
        Authorization: `Bearer ${HUBSPOT_TOKEN}`,
        "Content-Type": "application/json"
      },
      timeout: 10000
    }
  );
}

// =============================
// STATUS UPDATE
// =============================
async function updateStatus(contactId, status) {
  try {
    await axios.patch(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
      { properties: { ai_email_step_status: status } },
      {
        headers: {
          Authorization: `Bearer ${HUBSPOT_TOKEN}`,
          "Content-Type": "application/json"
        },
        timeout: 5000
      }
    );
  } catch (err) {
    console.error(`Status update failed for ${contactId}:`, err.message);
  }
}

// =============================
// SERVER STARTUP
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 SMB HubSpot Nurture worker running on port ${PORT}`);
  console.log(`⚡ Concurrency: ${CONCURRENCY} simultaneous jobs`);
});
