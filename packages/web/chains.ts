import { defineChain } from 'viem'
import { arbitrum, base, fantom, gnosis, mainnet, optimism, polygon } from 'viem/chains'

export const customChains = {
  sonic: /*#__PURE__*/ defineChain({
    id: 146,
    name: 'Sonic',
    nativeCurrency: {
      decimals: 18,
      name: 'Sonic',
      symbol: 'S',
    },
    rpcUrls: {
      default: { http: ['https://rpc.soniclabs.com'] },
    },
    blockExplorers: {
      default: {
        name: 'Sonic Explorer',
        url: 'https://sonicscan.org/',
      },
    },
    contracts: {
      multicall3: {
        address: '0xca11bde05977b3631167028862be2a173976ca11',
        blockCreated: 60,
      },
    },
    testnet: false,
  }),

  mode: /*#__PURE__*/ defineChain({
    id: 34443,
    name: 'Mode Mainnet',
    nativeCurrency: { name: 'Ether', symbol: 'ETH', decimals: 18 },
    rpcUrls: {
      default: {
        http: ['https://mainnet.mode.network'],
      },
    },
    blockExplorers: {
      default: {
        name: 'Mode Explorer',
        url: 'https://explorer.mode.network',
      },
    },
    contracts: {
      multicall3: {
        address: '0xca11bde05977b3631167028862be2a173976ca11',
        blockCreated: 2465882,
      },
    },
  }),

  bera: /*#__PURE__*/ defineChain({
    id: 80094,
    name: 'Berachain',
    nativeCurrency: {
      decimals: 18,
      name: 'BERA Token',
      symbol: 'BERA',
    },
    rpcUrls: {
      default: { http: ['https://rpc.berachain.com'] },
    },
    blockExplorers: {
      default: {
        name: 'Berascan',
        url: 'https://berascan.com',
      },
    },
    contracts: {
      multicall3: {
        address: '0xcA11bde05977b3631167028862bE2a173976CA11',
        blockCreated: 0,
      },
    },
    testnet: false,
  }),
  plumeTestnet: /*#__PURE__*/ defineChain({
    id: 98_867,
    name: 'Plume Testnet',
    testnet: true,
    nativeCurrency: {
      name: 'Plume',
      symbol: 'PLUME',
      decimals: 18,
    },
    rpcUrls: {
      default: {
        http: ['https://testnet-rpc.plume.org'],
        webSocket: ['wss://testnet-rpc.plume.org'],
      },
    },
    blockExplorers: {
      default: {
        name: 'Blockscout',
        url: 'https://testnet-explorer.plume.org',
        apiUrl: 'https://testnet-explorer.plume.org/api',
      },
    },
    contracts: {
      multicall3: {
        address: "0xcA11bde05977b3631167028862bE2a173976CA11",
        blockCreated: 199712,
      },
    },
    sourceId: 11_155_111
  }),

  plume: /*#__PURE__*/ defineChain({
    id: 98_866,
    name: 'Plume Mainnet',
    nativeCurrency: {
      name: 'Plume',
      symbol: 'PLUME',
      decimals: 18,
    },
    rpcUrls: {
      default: {
        http: ['https://rpc.plume.org'],
        webSocket: ['wss://rpc.plume.org'],
      },
    },
    blockExplorers: {
      default: {
        name: 'Blockscout',
        url: 'https://explorer.plume.org',
        apiUrl: 'https://explorer.plume.org/api',
      },
    },
    contracts: {
      multicall3: {
        address: '0xcA11bde05977b3631167028862bE2a173976CA11',
        blockCreated: 48_577,
      },
    },
  })
}

const chains = [
  customChains.plume,
  customChains.plumeTestnet
]

export default chains
