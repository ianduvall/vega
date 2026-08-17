import tape from 'tape';
import * as vega from '../index.js';
import {inherits} from 'vega-util';

// A transform whose evaluate returns a promise. Load is the only core
// transform that does so today, and it always resolves to a pulse, so an
// asynchronous StopPropagation is not otherwise reachable from the registry.
function AsyncSource(tuples) {
  vega.Transform.call(this, tuples, {});
  this.stop = false;
}

inherits(AsyncSource, vega.Transform, {
  transform(_, pulse) {
    if (this.stop) return Promise.resolve(pulse.StopPropagation);

    const out = pulse.fork(pulse.NO_SOURCE | pulse.NO_FIELDS);
    out.source = out.add = this.value;
    return Promise.resolve(out);
  }
});

tape('Transform does not store StopPropagation as an async operator pulse', async t => {
  const df = new vega.Dataflow(),
        tuples = [vega.ingest({v: 1}), vega.ingest({v: 2})],
        op = df.add(new AsyncSource(tuples));

  await df.runAsync();
  t.equal(op.pulse.source, tuples, 'a productive async run stores its pulse');

  // StopPropagation is a sentinel, not a pulse. An operator that stops
  // propagation still holds the tuples its last real pulse described, and
  // dataflow/update.js reads op.pulse.source to learn what those tuples are
  // when a later changeset is applied. Storing the sentinel there reports the
  // operator as empty and silently strands its data.
  op.stop = true;
  df.touch(op);
  await df.runAsync();

  t.notEqual(
    op.pulse,
    vega.Pulse.prototype.StopPropagation,
    'the sentinel is not stored as the operator pulse'
  );
  t.equal(op.pulse.source, tuples, 'the last real pulse survives a stopped run');

  t.end();
});

tape('Transform stores the resolved pulse of a productive async run', async t => {
  const df = new vega.Dataflow(),
        first = [vega.ingest({v: 1})],
        second = [vega.ingest({v: 2})],
        op = df.add(new AsyncSource(first));

  await df.runAsync();
  t.equal(op.pulse.source, first, 'first async pulse stored');

  op.value = second;
  df.touch(op);
  await df.runAsync();

  t.equal(op.pulse.source, second, 'the operator pulse advances with each run');
  t.end();
});
