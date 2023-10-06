import "dotenv/config"

const AUTH_TOKEN = process.env.TOKEN
const SOURCE_IDS = "199226"
const EXTENSION_VERSION = "2.3.13"

const fetchLogs = async (query, count = 100, from = (Date.now() - 1000 * 60 * 60 * 12), to = Date.now()) => {
  query = encodeURIComponent(query)
  from = new Date(from).toISOString()
  to = new Date(to).toISOString()

  const response = await fetch(
    `https://logs.betterstack.com/api/v1/query?source_ids=${SOURCE_IDS}&batch=${count}&from=${from}&to=${to}&query=${query}`, {
      headers: {
        "Authorization": `Bearer ${AUTH_TOKEN}`
      }
    }
  )

  return await response.json()
}

const getOFRequestsBefore = async (creatorid, dt) => {
  const startTime = dt - 1000 * 60 * 3
  const logs = await fetchLogs(`creatorid="${creatorid}" extensionVersion="${EXTENSION_VERSION}" "OF request"`, 1000, startTime, dt)
  const data = {}

  for (const { message } of logs.data) {
    const regex = /:\s(\[.*\])\s/g
    const result = regex.exec(message)
    const service = result ? result[1] : "NO_SERVICE_SPECIFIED"

    if (result === null) console.log(result, message)

    data[service] = (data[service] ?? 0) + 1
  }

  return data
}

const json = await fetchLogs(`message_json.status="429" extensionVersion="${EXTENSION_VERSION}"`)
const data = json.data
  .map((e) => ({ email: JSON.parse(e.json).email, creatorid: JSON.parse(e.json).creatorId, dt: e._dt }))

// const checked = []
const result = []

let i = 0

for (const { email, creatorid, dt } of data) {
  const millis = dt / 1000
  const data = await getOFRequestsBefore(creatorid, millis)

  console.log(i++)

  result.push({ email, creatorid, dt: new Date(millis).toString(), data })
}

console.log(result)
