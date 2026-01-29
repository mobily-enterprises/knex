const { expect } = require('chai');
const sinon = require('sinon');
const Client = require('../../../lib/client');

class TestClient extends Client {
  constructor(config = {}) {
    super({ ...config, client: 'test' });
    this.driverName = null;
  }

  validateConnection() {
    return true;
  }
}

describe('Client connection routing', () => {
  afterEach(() => {
    sinon.restore();
  });

  it('routes connections via connectionResolver', async () => {
    const client = new TestClient({
      connections: {
        master: { connection: { host: 'master' } },
        replica: { connection: { host: 'replica' } },
      },
      connectionResolver: (context) =>
        context.method === 'select' ? 'replica' : 'master',
    });

    const masterConn = {};
    const replicaConn = {};

    const masterPool = {
      acquire: sinon.stub().returns({ promise: Promise.resolve(masterConn) }),
      release: sinon.stub().returns(true),
    };
    const replicaPool = {
      acquire: sinon.stub().returns({ promise: Promise.resolve(replicaConn) }),
      release: sinon.stub().returns(true),
    };

    client.pools = { master: masterPool, replica: replicaPool };
    client.pool = masterPool;

    const readConn = await client.acquireConnection({ method: 'select' });
    expect(readConn).to.equal(replicaConn);
    expect(readConn.__knexConnectionName).to.equal('replica');
    await client.releaseConnection(readConn);
    expect(replicaPool.release.calledOnce).to.be.true;

    const writeConn = await client.acquireConnection({ method: 'insert' });
    expect(writeConn).to.equal(masterConn);
    expect(writeConn.__knexConnectionName).to.equal('master');
    await client.releaseConnection(writeConn);
    expect(masterPool.release.calledOnce).to.be.true;
  });

  it('resolves dynamic connection settings per call', async () => {
    const resolver = sinon
      .stub()
      .onCall(0)
      .returns({ host: 'replica-1' })
      .onCall(1)
      .returns({ host: 'replica-2' });

    const client = new TestClient({
      connections: {
        replica: { connection: resolver },
      },
    });

    const first = await client._resolveConnectionSettings('replica');
    const second = await client._resolveConnectionSettings('replica');

    expect(resolver.calledTwice).to.be.true;
    expect(first.host).to.equal('replica-1');
    expect(second.host).to.equal('replica-2');
  });

  it('parses connection strings in named connections', async () => {
    const client = new TestClient({
      connections: {
        primary: 'postgres://user:pass@localhost:5432/mydb',
      },
    });

    const settings = await client._resolveConnectionSettings('primary');

    expect(settings.host).to.equal('localhost');
    expect(settings.user).to.equal('user');
    expect(settings.password).to.equal('pass');
    expect(settings.database).to.equal('mydb');
  });

  it('throws when connection and connections are both set', () => {
    expect(
      () =>
        new TestClient({
          connection: { host: 'legacy' },
          connections: {
            master: { connection: { host: 'master' } },
          },
        })
    ).to.throw(
      'knex: cannot specify both `connection` and `connections` in the configuration'
    );
  });

  it('throws when multiple connections have no resolver or default', () => {
    expect(
      () =>
        new TestClient({
          connections: {
            one: { connection: { host: 'one' } },
            two: { connection: { host: 'two' } },
          },
        })
    ).to.throw(
      'knex: multiple connections configured without a default; please provide a connectionResolver or a `default`/`master` connection'
    );
  });

  it('throws when connectionResolver returns non-string', async () => {
    const client = new TestClient({
      connections: {
        master: { connection: { host: 'master' } },
      },
      connectionResolver: () => 123,
    });
    const pool = {
      acquire: sinon.stub().returns({ promise: Promise.resolve({}) }),
      release: sinon.stub().returns(true),
    };
    client.pools = { master: pool };
    client.pool = pool;

    try {
      await client.acquireConnection({ method: 'select' });
      throw new Error('expected acquireConnection to throw');
    } catch (err) {
      expect(err.message).to.equal(
        'knex: connectionResolver must return a non-empty string when provided'
      );
    }
  });
});
