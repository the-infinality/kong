import { arbitrum, base, fantom, mainnet, optimism } from 'viem/chains'
import { formatUnits, parseAbi, parseUnits } from 'viem'
import { cache } from 'lib/cache'
import { rpcs } from './rpcs'
import db from './db'
import { Price, PriceSchema } from 'lib/types'
import { mq } from 'lib'
import { getBlockNumber, getBlockTime } from 'lib/blocks'
import { z } from 'zod'
import * as yaml from 'js-yaml'
import * as fs from 'fs'
import path from 'path'

export const lens = {
  [mainnet.id]: '0x83d95e0D5f402511dB06817Aff3f9eA88224B030' as `0x${string}`,
  [optimism.id]: '0xB082d9f4734c535D9d80536F7E87a6f4F471bF65' as `0x${string}`,
  [fantom.id]: '0x57AA88A0810dfe3f9b71a9b179Dd8bF5F956C46A' as `0x${string}`,
  [base.id]: '0xE0F3D78DB7bC111996864A32d22AB0F59Ca5Fa86' as `0x${string}`,
  [arbitrum.id]: '0x043518AB266485dC085a1DB095B8d9C2Fc78E9b9' as `0x${string}`
}

const PricesYamlConfigSchema = z.object({
  spork: z.array(z.object({
    chainId: z.number(),
    address: z.string(),
    assetId: z.string(),
    defaultPrice: z.string().optional()
  })),
  eoracle: z.record(
    z.string(), // chainId as string key
    z.record(
      z.string(), // address as string key
      z.object({
        address: z.string()
      })
    )
  ),
})

const yamlPath = (() => {
  const local = path.join(__dirname, '../../config', 'prices.local.yaml')
  const production = path.join(__dirname, '../../config', 'prices.yaml')
  if (fs.existsSync(local)) return local
  return production
})()

const yamlFile = fs.readFileSync(yamlPath, 'utf8')
const pricesConfig = PricesYamlConfigSchema.parse(yaml.load(yamlFile))


export async function fetchErc20PriceUsd(chainId: number, token: `0x${string}`, blockNumber?: bigint, latest = false): Promise<{ priceUsd: number, priceSource: string }> {
  if (!blockNumber) {
    blockNumber = await getBlockNumber(chainId)
    latest = true
  }

  return cache.wrap(`fetchErc20PriceUsd:${chainId}:${token}:${blockNumber}`, async () => {
    return await __fetchErc20PriceUsd(chainId, token, blockNumber!, latest)
  }, 30_000)
}

async function __fetchErc20PriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint, latest = false) {
  let result: Price | undefined

  if (latest) {
    result = await fetchEOraclePriceUsd(chainId, token, blockNumber, latest)
    await mq.add(mq.job.load.price, result)
    if (result) return result

    result = await fetchYDaemonPriceUsd(chainId, token, blockNumber)
    await mq.add(mq.job.load.price, result)
    if (result) return result
  }

  result = await fetchDbPriceUsd(chainId, token, blockNumber)
  if (result) return result

  result = await fetchEOraclePriceUsd(chainId, token, blockNumber)
  await mq.add(mq.job.load.price, result)
  if (result) return result

  result = await fetchLensPriceUsd(chainId, token, blockNumber)
  if (result) {
    await mq.add(mq.job.load.price, result)
    return result
  }

  if (JSON.parse(process.env.YPRICE_ENABLED || 'false')) {
    result = await fetchYPriceUsd(chainId, token, blockNumber)
    if (result) {
      await mq.add(mq.job.load.price, result)
      return result
    }
  }

  console.warn('🚨', 'no price', chainId, token, blockNumber)
  const empty = { chainId, address: token, priceUsd: 0, priceSource: 'na', blockNumber, blockTime: await getBlockTime(chainId, blockNumber) }
  await mq.add(mq.job.load.price, empty)
  return empty
}


async function fetchEOraclePriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint, latest = false) {
  const config = pricesConfig.eoracle[chainId.toString()][token.toLowerCase()]
  if (config === undefined) return undefined

  console.log('🔍', 'Retrieving eOracle price for', chainId, token, blockNumber, "from:", config.address)
  try {
    const decimals = await cachedEOracleDecimals(chainId, config.address as `0x${string}`)

    if (latest) {
      blockNumber = await getBlockNumber(chainId)
    }

    const price = await rpcs.next(chainId, blockNumber).readContract({
      address: config.address as `0x${string}`,
      functionName: 'latestAnswer',
      args: [],
      abi: parseAbi(['function latestAnswer() view returns (uint256)']),
      blockNumber
    }) as bigint

    console.log('🔍', 'eOracle price retrieved', chainId, token, blockNumber, price)

    if (price === 0n) return undefined

    return PriceSchema.parse({
      chainId,
      address: token,
      priceUsd: formatUnits(price, decimals),
      priceSource: 'eoracle',
      blockNumber,
      blockTime: await getBlockTime(chainId, blockNumber)
    })

  } catch (error) {
    console.log('🔍', 'eOracle price failed', chainId, token, blockNumber, "error:", error)
    return undefined
  }
}

async function cachedEOracleDecimals(chainId: number, token: `0x${string}`): Promise<number> {
  const cacheKey = `eOracleDecimals:${chainId}:${token}`
  return await cache.wrap(cacheKey, async () => {
    return await rpcs.next(chainId).readContract({
      address: pricesConfig.eoracle[chainId.toString()][token.toLowerCase()]!.address as `0x${string}`,
      functionName: 'decimals',
      args: [],
      abi: parseAbi(['function decimals() view returns (uint8)']),
    }) as number
  }, 2_592_000_000) // 30 days in milliseconds
}

async function fetchSporkPriceUsdCached(chainId: number, token: `0x${string}`, blockNumber: bigint) {
  const cacheKey = `sporkPriceUsd:${chainId}:${token}`
  const result = await cache.wrap(cacheKey, async () => {
    return await fetchSporkPriceUsd(chainId, token, blockNumber)
  }, 60_000)

  // update block number and time to match the block number and time of the price requested
  result.blockNumber = blockNumber
  result.blockTime = await getBlockTime(chainId, blockNumber)
  return result
}

async function fetchSporkPriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint) {
  if (!process.env.SPORK_API || !process.env.SPORK_API_AUTH) return undefined

  const asset = pricesConfig.spork.find(spork => spork.chainId === chainId && spork.address === token)
  if (!asset) return undefined

  const assetId = asset.assetId


  let price
  if (assetId) {
    const url = `${process.env.SPORK_API}/v1/prices/latest?assets=${assetId}`
    const result = await fetch(url, {
      headers: {
        'Authorization': `${process.env.SPORK_API_AUTH}`
      }
    })
    const json = await result.json()

    if (result.status === 200) price = json.data[assetId]['price']
    if (result.status === 404 && asset.defaultPrice) price = BigInt(asset.defaultPrice)
  } else if (asset.defaultPrice) {
    price = BigInt(asset.defaultPrice)
  } else {
    return undefined
  }

  if (!price) return undefined

  return PriceSchema.parse({
    chainId,
    address: token,
    priceUsd: Number(formatUnits(price, 18)),
    priceSource: 'spork',
    blockNumber,
    blockTime: await getBlockTime(chainId, blockNumber)
  })
}

async function fetchYPriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint) {
  if (!process.env.YPRICE_API) return undefined

  try {
    const url = `${process.env.YPRICE_API}/get_price/${chainId}/${token}?block=${blockNumber}`
    const result = await fetch(url, {
      headers: {
        'X-Signature': process.env.YPRICE_API_X_SIGNATURE || '',
        'X-Signer': process.env.YPRICE_API_X_SIGNER || ''
      }
    })

    const priceUsd = Number(await result.json())
    if (priceUsd === 0) return undefined

    return PriceSchema.parse({
      chainId,
      address: token,
      priceUsd,
      priceSource: 'lens',
      blockNumber,
      blockTime: await getBlockTime(chainId, blockNumber)
    })

  } catch (error) {
    console.warn('🚨', 'yprice failed', chainId, token, blockNumber)
    return undefined
  }
}

async function fetchDbPriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint) {
  const result = await db.query(
    `SELECT
      chain_id as "chainId",
      address,
      price_usd as "priceUsd",
      price_source as "priceSource",
      block_number as "blockNumber",
      block_time as "blockTime"
    FROM price WHERE chain_id = $1 AND address = $2 AND block_number = $3`,
    [chainId, token, blockNumber]
  )
  if (result.rows.length === 0) return undefined
  return PriceSchema.parse(result.rows[0])
}

async function fetchLensPriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint) {
  if (!(chainId in lens)) return undefined

  try {
    const priceUSDC = await rpcs.next(chainId, blockNumber).readContract({
      address: lens[chainId as keyof typeof lens],
      functionName: 'getPriceUsdcRecommended',
      args: [token],
      abi: parseAbi(['function getPriceUsdcRecommended(address tokenAddress) view returns (uint256)']),
      blockNumber
    }) as bigint

    if (priceUSDC === 0n) return undefined

    return PriceSchema.parse({
      chainId,
      address: token,
      priceUsd: Number(priceUSDC * 10_000n / BigInt(10 ** 6)) / 10_000,
      priceSource: 'lens',
      blockNumber,
      blockTime: await getBlockTime(chainId, blockNumber)
    })

  } catch (error) {
    console.warn('🚨', 'lens price failed', error)
    return undefined
  }
}

async function fetchAllYDaemonPrices() {
  if (!process.env.YDAEMON_API) throw new Error('!YDAEMON_API')
  return cache.wrap('fetchAllYDaemonPrices', async () => {
    const url = `${process.env.YDAEMON_API}/prices/all?humanized=true`
    const result = await fetch(url)
    const json = await result.json()
    return lowercaseAddresses(json)
  }, 60_000)
}

type YDaemonPrices = {
  [key: string]: {
    [key: string]: number
  }
}

function lowercaseAddresses(data: YDaemonPrices): YDaemonPrices {
  const result: YDaemonPrices = {}
  for (const outerKey in data) {
    result[outerKey] = {}
    for (const innerKey in data[outerKey]) {
      result[outerKey][innerKey.toLowerCase()] = data[outerKey][innerKey]
    }
  }
  return result
}

async function fetchYDaemonPriceUsd(chainId: number, token: `0x${string}`, blockNumber: bigint) {
  try {
    const prices = await fetchAllYDaemonPrices()
    const price = prices[chainId.toString()]?.[token.toLowerCase()] || 0
    if (isNaN(price)) return undefined
    return PriceSchema.parse({
      chainId,
      address: token,
      priceUsd: price,
      priceSource: 'ydaemon',
      blockNumber,
      blockTime: await getBlockTime(chainId, blockNumber)
    })
  } catch (error) {
    console.warn('🚨', 'ydaemon price failed', error)
    return undefined
  }
}
