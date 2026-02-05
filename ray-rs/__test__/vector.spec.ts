import test from 'ava'

import { createVectorIndex } from '../dist/index.js'

test('vector index trains and searches', (t) => {
  const index = createVectorIndex({
    dimensions: 4,
    trainingThreshold: 2,
    ivf: { nClusters: 1, nProbe: 1 },
  })

  index.set(1, [1, 0, 0, 0])
  index.set(2, [0, 1, 0, 0])
  index.buildIndex()

  const stats = index.stats()
  t.true(stats.indexTrained)
  t.is(stats.indexClusters, 1)

  const hits = index.search([1, 0, 0, 0], { k: 1 })
  t.is(hits.length, 1)
  t.is(hits[0].nodeId, 1)
})

test('vector index validates dimensions and values', (t) => {
  const index = createVectorIndex({ dimensions: 4 })

  t.throws(() => index.set(1, [1, 0, 0]), { message: /expected 4, got 3/i })
  t.throws(() => index.set(2, [1, Number.NaN, 0, 0]), { message: /invalid vector/i })
})

