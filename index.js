import * as fs from 'fs';
import "dotenv/config"
import {rateLimitQueue} from "./rate-limit-queue.js";

const AUTH_TOKEN = process.env.TOKEN
const SOURCE_IDS = "199226"

const fetchLogs = async (query, count = 100, from, to) => {
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

  const text = await response.text();
  let object;

  try {
    object = JSON.parse(text);
  } catch (error) {
    console.log("Error while parsing json: " + error);
    console.log("Text: " + text);
    throw error;
  }

  return {
    ...object, data: object.data.map(log => {
      let date = parseInt(log._dt);

      //if(date > Date.now() + 24 * 1000 * 60 * 60) {
      date = Math.trunc(date / 1000);
      //}

      return {...log, date: date, parsedJson: JSON.parse(log.json)};
    })
  };
}

const fetchAllLogsUntil = async (query, from, to = Date.now()) => {
  return rateLimitQueue.add(async () => {
    const logs = [];
    let response = null;

    while (!response || response.data.length > 0) {
      response = await (response ? fetchLogsByUrl(response.pagination.next) : fetchLogs(query, 1000, from, to));

      logs.push(...response.data.filter(e => e.date >= from))

      if (response.data.find(e => e.date < from)) {
        break;
      }
    }

    return logs;
  });
}

async function getAllRateLimitedLogs() {
  const path = './all-rate-limited.json';

  if (fs.existsSync(path)) {
    // Read the file
    console.log("Reading all rate limited logs from existing file!")
    const fileContent = fs.readFileSync(path, 'utf8');
    return JSON.parse(fileContent);
  } else {
    console.log('File does not exist.');
    const arrayToWrite = await fetchAllLogsUntil(`message_json.status="429" AND email=\"Maximilianschulz1@web.de\"`, Date.now() - 24 * 60 * 60 * 1000);
    fs.writeFileSync(path, JSON.stringify(arrayToWrite));
    console.log('Written new array to file.');
    return arrayToWrite;
  }
}

const getOFRequestsBefore = async (creatorid, occurrenceDate) => {
  const startTime = occurrenceDate - 1000 * 60 * 15
  const logs = await fetchAllLogsUntil(`creatorid="${creatorid}" "OF request"`, startTime, occurrenceDate)
  const data = {}

  for (const {message, date: logDate, parsedJson} of logs) {
    const regex = /:\s(\[.*\])\s/g
    const result = regex.exec(message)
    const service = (result ? result[1] : "NO_SERVICE_SPECIFIED") + "_" + parsedJson.extensionVersion;
    const array = data[service] ?? [0, 0, 0, 0, 0];

    // 1, 3, 5, 10, 15

    if (Math.abs(logDate - startTime) < 1000 * 60) array[0] = array[0] + 1
    if (Math.abs(logDate - startTime) < 1000 * 60 * 3) array[1] = array[1] + 1
    if (Math.abs(logDate - startTime) < 1000 * 60 * 5) array[2] = array[2] + 1
    if (Math.abs(logDate - startTime) < 1000 * 60 * 10) array[3] = array[3] + 1
    if (Math.abs(logDate - startTime) < 1000 * 60 * 15) array[4] = array[4] + 1

    data[service] = array;
  }

  return data
}

const allRateLimitedLogs = await getAllRateLimitedLogs();
const occurrences = [];

console.log("Found " + allRateLimitedLogs.length + " rate limited requests. Handling occurrences...");

let i = 0;

for (const {parsedJson, date} of allRateLimitedLogs) {
  console.log(`Handling log ${++i}/${allRateLimitedLogs.length}...`)

  const minDate = date - 1000 * 60 * 3;

  if (allRateLimitedLogs.find(e => e.date >= minDate && e.date < date && e.parsedJson.creatorId === parsedJson.creatorId)) {
    continue;
  }

  occurrences.push({
    email: parsedJson.email,
    creatorId: parsedJson.creatorId,
    date
  });
}

console.log("Found " + occurrences.length + " total occurrences. Now checking all of them...")

// const checked = []
const result = {};

i = 0

async function getOccurrenceInfo(occurrenceResult) {
  const {email, creatorId, date} = occurrenceResult;
  const occurrenceData =
    Object.entries(await getOFRequestsBefore(creatorId, date))
      .sort(([aKey, aValue], [bKey, bValue]) => bValue[3] - aValue[3]);
  const occurrenceDataObject = {};

  for (const [key, value] of occurrenceData) {
    occurrenceDataObject[key] = value;
  }

  console.log("Handled " + (++i) + "/" + occurrences.length + " occurrences");

  const totalRequests = occurrenceData.map(([key, value]) => value)
    .reduce((a, b) =>
        [
          a[0] + b[0],
          a[1] + b[1],
          a[2] + b[2],
          a[3] + b[3],
          a[4] + b[4]],
      [0, 0, 0, 0, 0]
    );
  return {
    totalRequests,
    creatorId,
    email,
    date: new Date(date).toISOString(),
    requests: occurrenceDataObject
  };
}

const allOccurrences = await Promise.all(occurrences.map(getOccurrenceInfo));

for (const occurrence of allOccurrences) {
  const creatorData = result[occurrence.creatorId] ?? {
    email: occurrence.email,
    creatorId: occurrence.creatorId,
    total: 0,
    occurrences: []
  }
  creatorData.total = creatorData.total + occurrence.totalRequests[4];
  creatorData.occurrences.push(occurrence);
  creatorData.occurrences.sort((a, b) => b.totalRequests[4] - a.totalRequests[4]);

  result[occurrence.creatorId] = creatorData;
}


const finalResult = Object.values(result).sort((a, b) => b.total - a.total);

fs.writeFileSync('output-' + Date.now() + '.json', JSON.stringify(finalResult));
console.log("Finished and written to file!")
