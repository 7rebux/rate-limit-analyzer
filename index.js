import "dotenv/config"

const AUTH_TOKEN = process.env.TOKEN
const SOURCE_IDS = "199226"

const fetchLogs = async (query, count = 100, from = (Date.now() - 1000 * 60 * 60 * 12), to = Date.now()) => {
  query = encodeURIComponent(query)
  from = new Date(from).toISOString()
  to = new Date(to).toISOString()

  return fetchLogsByUrl(`https://logs.betterstack.com/api/v1/query?source_ids=${SOURCE_IDS}&batch=${count}&from=${from}&to=${to}&query=${query}`);
}

const fetchLogsByUrl = async (url) => {
  const response = await fetch(
    url, {
      headers: {
        "Authorization": `Bearer ${AUTH_TOKEN}`
      }
    }
  )

  return await response.json()
}

const fetchAllLogsUntil = async (query, until = Date.now() - 24 * 60 * 60 * 1000) => {
  const logs = [];
  let response = null;

  while (!response || response.data.length > 0) {
    response = await (response ? fetchLogsByUrl(response.pagination.next) : fetchLogs(query, 1000, until));

    logs.push(...response.data.filter(e => Date.parse(e.dt) >= until))

    if (response.data.find(e => Date.parse(e.dt) < until)) break;
  }

  return logs;
}

const getOFRequestsBefore = async (creatorid, dt) => {
  const startTime = dt - 1000 * 60 * 3
  const logs = await fetchLogs(`creatorid="${creatorid}" "OF request"`, 1000, startTime, dt)
  const data = {}

  for (const {message, extensionVersion} of logs.data) {
    const regex = /:\s(\[.*\])\s/g
    const result = regex.exec(message)
    const service = (result ? result[1] : "NO_SERVICE_SPECIFIED") + "_" + extensionVersion;

    data[service] = (data[service] ?? 0) + 1
  }

  return data
}

const allRateLimitedLogs = await fetchAllLogsUntil(`message_json.status="429" AND (extensionVersion="2.3.13" OR extensionVersion="2.3.14")`);
const occurrences = [];

console.log("Found " + allRateLimitedLogs.length + " rate limited requests. Handling occurrences...");

for (const log of allRateLimitedLogs) {
  const logDate = Date.parse(log.dt);
  const minDate = logDate - 1000 * 60 * 3;
  const parsedJson = JSON.parse(log.json);

  if (allRateLimitedLogs.filter(e => {
    const otherDate = Date.parse(e.dt);
    return otherDate >= minDate && otherDate < logDate;
  }).find(e => {
    const otherJson = JSON.parse(e.json);
    return otherJson.creatorId === parsedJson.creatorId;
  })) {
    continue;
  }

  occurrences.push({
    email: parsedJson.email,
    creatorId: parsedJson.creatorId,
    dt: logDate
  });
}

console.log("Found " + occurrences.length + " total occurrences. Now checking all of them...")

// const checked = []
const result = {};

let i = 0

for (const {email, creatorId, dt} of occurrences) {
  const creatorData = result[creatorId] ?? {email, creatorId, total: 0, occurrences: []}

  const occurrenceData = await getOFRequestsBefore(creatorId, dt);
  const totalRequests = Object.values(occurrenceData).reduce((a, b) => a + b, 0);
  creatorData.total = creatorData.total + totalRequests;

  const occurrence = {
    totalRequests,
    requests: Object.entries(occurrenceData)
      .sort(([aKey, aValue], [bKey, bValue]) => bValue - aValue)
      .map(([key, value]) => `${key}: ${value}`)
  };
  creatorData.occurrences.push(occurrence);
  creatorData.occurrences.sort((a, b) => b.totalRequests - a.totalRequests);

  result[creatorId] = creatorData;

  console.log("Handled " + (++i) + "/" + occurrences.length + " occurrences");
}

const finalResult = Object.values(result).sort((a, b) => b.total - a.total);
console.log(JSON.stringify(finalResult))
