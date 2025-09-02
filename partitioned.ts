import { setDriver } from '@riddance/docs/driver'
import { Driver } from './driver.js'

export * from '@riddance/docs'

setDriver(new Driver())
