const puppeteer = require('puppeteer');
const process = require('process');

(async () => {
  const browser = await puppeteer.launch({args: ['--no-sandbox', '--disable-setuid-sandbox']});
  console.log(browser.wsEndpoint());
  
})();
