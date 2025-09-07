import { z } from 'zod'
import { toEventSelector, parseAbi } from 'viem'
import { priced, div } from 'lib/math'
import { fetchOrExtractDecimals, fetchOrExtractAssetAddress } from '../../lib'
import { fetchErc20PriceUsd } from '../../../../prices'
import { first } from '../../../../db'
import { ThingSchema } from 'lib/types'
import { rpcs } from '../../../../rpcs'

export const topics = [
  'event Transfer(address indexed sender, address indexed receiver, uint256 value)'
].map(e => toEventSelector(e))

export default async function process(chainId: number, address: `0x${string}`, data: object) {
  const { blockNumber, args } = z.object({
    blockNumber: z.bigint({ coerce: true }),
    args: z.object({
      value: z.bigint({ coerce: true })
    })
  }).parse(data)

  // Check if this is a vault
  const vault = await first<{ chainId: number; address: `0x${string}`; label: string; defaults: any }>(
    ThingSchema,
    'SELECT * FROM thing WHERE chain_id = $1 AND address = $2 AND label = $3',
    [chainId, address, 'vault']
  )

  if (vault) {
    // This is a vault - get pricePerShare and asset price
    const decimals = vault.defaults.decimals

    // Get pricePerShare from vault
    const pricePerShareRaw = await rpcs.next(chainId, blockNumber).readContract({
      address,
      abi: parseAbi(['function pricePerShare() view returns (uint256)']),
      functionName: 'pricePerShare',
      blockNumber
    }) as bigint

    const pricePerShare = div(pricePerShareRaw, 10n ** BigInt(decimals))

    // Get asset address and its price
    const assetAddress = vault.defaults.asset as `0x${string}`
    const assetPrice = await fetchErc20PriceUsd(chainId, assetAddress, blockNumber)
    const resolvedPrice = assetPrice.priceUsd * pricePerShare

    return {
      valueUsd: priced(args.value, decimals, resolvedPrice),
      priceUsd: resolvedPrice,
      priceSource: "computed-" + assetPrice.priceSource,
    }
  } else {
    // This is a regular ERC20 token
    const decimals = await fetchOrExtractDecimals(chainId, address)
    const price = await fetchErc20PriceUsd(chainId, address, blockNumber)
    return {
      valueUsd: priced(args.value, decimals, price.priceUsd),
      priceUsd: price.priceUsd,
      priceSource: price.priceSource,
    }
  }
}
