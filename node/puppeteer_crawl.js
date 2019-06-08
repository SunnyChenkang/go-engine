const puppeteer = require('puppeteer');
const process = require('process');

var args = process.argv.splice(2);

(async () => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();
  await page.goto(args[0]);
  console.log(await page.content());
  
  await browser.close();
})();