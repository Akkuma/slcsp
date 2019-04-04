
import { createReadStream } from 'fs';
import { EOL } from 'os';
import { createInterface } from 'readline';

// Minor helper function to avoid function nesting.
// Similiar to what is found in languages like Elixir x |> yFn |> zFn = zFn(yFn(x))
function pipe(...fns) {
  return (...args) => fns.reduce((res, fn, i) => fn.apply(this, i === 0 ? res : [res]), args)
}

const trueFn = _ => true;
/*
  Reads a csv line by line turning it into an object, while optionally filtering out rows for efficiency

  @returns Promise<{ headers: [], rows: [] }>
*/
export function parseCSV(csv, filterSets) {
  const rl = createInterface({
    input: createReadStream(`${csv}.csv`),
    crlfDelay: Infinity,
  });

  const filter = filterSets ? createQueryFilter(filterSets) : trueFn;
  const sheet = { headers: [], rows: []};
  rl.on('line', (line) => {
    const trimmed = line.trim();
    const row = trimmed.split(',');

    if (sheet.headers.length) {
      const rowObj = pipe(convertRowToObj, filterRowObj(filter))(sheet.headers, row);
      if (rowObj) sheet.rows.push(rowObj);
    }
    else {
      sheet.headers = row;
    }
  });

  return new Promise(res => rl.on('close', _ => res(sheet)))
}

// Converts a simple [x,y,z] into key, vals on an object
const convertRowToObj = (headers, row) => row.reduce((acc, val, i) => (acc[headers[i]] = val, acc), {});

// Filters rows to only those that meet the criteria
const filterRowObj = filter => row => {
  if (filter(row)) return row;
}

/*
  Takes an array of headers to create a filter set.
  Filter sets are [key, val], which will map over to a header and the value for that column.
  This mimics something akin to a SQL join x.col = y.col.

  @returns [[key1, val1], [key2, val2]]
*/
function createJoinMapper(joins) {
  return row => joins.map(join => [join, row[join]]);
}

/*
  Takes an array of filter sets that creates a filter for a row that

   Filter Sets
  [
    [ [key1, val1] //filter , //and [key2, val2] //filter ], //filter set
    // or
    [ [key1, val1] //filter , //and [key2, val2] //filter ], //filter set
  ] //filter sets

  @returns row => Boolean
*/
export function createQueryFilter(filterSets) {
  return row => filterSets.some(filterSet => filterSet.every(([key, val]) => row[key] === val));
}

/*
  Takes multiple sheets and merges them into one object that will performing SQL like joins between sheets

  @returns { headers: {}, rows: [{}] }
*/
export function mergeSheets(sheetQueries) {
  let prevSheet;
  return sheetQueries.reduce((acc, sheetQuery) => {
    const { sheet, name } = sheetQuery

    // Passes along header information into merged sheet
    acc.headers[name] = sheet.headers;
    if(!prevSheet) {
      // Simple mapping as can't join sheets yet
      acc.rows = sheet.rows.map(_ => ({[name]: _}));
    }
    else {
      const { joins, name: prevName } = prevSheet;
      const joinMapper = createJoinMapper(joins);

      acc.rows = acc.rows.map(row => {
        let filter;

        if (Array.isArray(row[prevName])) {
          // Array of rows to create a filter set per row
          filter = createQueryFilter(row[prevName].map(joinMapper));
        }
        else {
          // A single row, but createQueryFilter requires an array of arrays
          filter = createQueryFilter([joinMapper(row[prevName])]);
        }

        // Continues to build up a row with previous data along with new data
        return { ...row, [name]: sheet.rows.filter(filter) };
      });
    }

    prevSheet = sheetQuery;

    return acc;
  }, { headers: {}, rows: [] });
}

/*
  Filter Sets are used by query filters and 
  this provides a painless way to create the most basic of filter sets
*/
function createSimpleFilterSets(key, val) {
  return [[[key, val]]]
}

//@NOTE: Could expose options to be passed into run for 
// csvs to parse, filter sets, joins, names, and additional business logic post merge
export async function run() {
  // Load all CSVs in parallel while waiting for them all to complete
  const [slcsps, plans, zips] = await Promise.all([
    parseCSV('slcsp'), 
    parseCSV('plans', createSimpleFilterSets('metal_level', 'Silver')), 
    parseCSV('zips'),
  ]);

  // Merge all sheets created from the parse
  const merged = mergeSheets([
    { sheet: slcsps, joins: ['zipcode'], name: 'slcsp' }, // join between slcsps.zipcode = zips.zipcode
    { sheet: zips, joins: ['state', 'rate_area'], name: 'zips' }, // join between zips.state = plans.state AND zips.rate_area = plans.rate_area
    { sheet: plans, name: 'plans' }
  ]);
   
  // Additional business logic independent of merging
  const filteredRows = merged.rows.map(row => {
    // Check if any plans after the current plan has the same rate and remove them
    row.plans = row.plans.filter((plan, i) => !row.plans.slice(++i).some(_ => _.rate === plan.rate));
    row.plans.sort((a, b) => Number.parseFloat(a.rate) - Number.parseFloat(b.rate));

    // Ensure that all zip matches belong to a single rate area otherwise it is ambigious
    const rateAreas = [];
    const isSingleRateArea = row.zips.every(({rate_area}) => {
      if (!rateAreas.includes(rate_area)) rateAreas.push(rate_area);

      return rateAreas.length === 1;
    });

    // Set the rate if it exists in only a single rate area and has at least 2 unique rate plans
    row.slcsp.rate = isSingleRateArea && row.plans[1] ? Number.parseFloat(row.plans[1].rate).toFixed(2) : '';

    return row;
  });

  // README specified process.stdout
  // console.log technically does that too under the hood, but stuck with explicit
  process.stdout.write(`${slcsps.headers.join(',')}${EOL}`);
  filteredRows.forEach(_ => process.stdout.write(`${_.slcsp.zipcode},${_.slcsp.rate ? Number.parseFloat(_.slcsp.rate).toFixed(2) : ''}${EOL}`));
}

run();