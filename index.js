require('dotenv').config();
const fs = require('fs');
const csv = require('csv-parser');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// Initialize API Keys
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const SERPAPI_KEY = process.env.SERPAPI_KEY;

// File Paths
const INPUT_FILE = 'locations.csv';
const OUTPUT_FILE = 'shops_output.csv';

// Setup Output CSV Headers
fs.writeFileSync(OUTPUT_FILE, '"Search Location","Name","Ratings","Best Services","Remarks","Shop Address","Website","Phone"\n');

// 1. Function to grab hard facts from SerpApi 
async function getShopsFromSerpApi(query) {
    const url = `https://serpapi.com/search.json?engine=google_local&q=${encodeURIComponent(query)}&api_key=${SERPAPI_KEY}`;
    try {
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.error) {
            console.log(`   [!] API Error: ${data.error}`);
            return [];
        }

        if (!data.local_results) return [];

        // Filter for shops with AT LEAST 250 reviews
        let qualifiedShops = data.local_results.filter(shop => shop.reviews && shop.reviews >= 250);

        // Sort by Stars first, then by Total Reviews as the tiebreaker
        qualifiedShops.sort((a, b) => {
            if (b.rating !== a.rating) return b.rating - a.rating; 
            return b.reviews - a.reviews; 
        });

        return qualifiedShops; 
        
    } catch (error) {
        console.error(`Error fetching data for ${query}:`, error.message);
        return [];
    }
}

// 2. Function to generate the "Brain Work" using Gemini (Gemma 3 27B)
async function generateShopDetails(shopName, shopType, snippet) {
    const model = genAI.getGenerativeModel({ model: 'gemma-3-27b-it' });
    
    const prompt = `
    Analyze this local business:
    Name: ${shopName}
    Type: ${shopType || 'Phone repair'}
    Description/Snippet: ${snippet || 'Provides local phone repair services.'}
    
    Task:
    1. Identify or infer the top 3 specific services they offer (e.g., Screen Repair, Battery Replacement, Water Damage).
    2. Write a 2-word remark based on typical high-rated shops (e.g., Fair Price, Fast Service, Premium Quality).
    
    Respond STRICTLY in this exact format with a pipe separator, nothing else:
    Services: [service 1, service 2, service 3] | Remark: [2-word remark]
    `;

    try {
        const result = await model.generateContent(prompt);
        const responseText = result.response.text().trim();
        
        const parts = responseText.split('|');
        const services = parts[0] ? parts[0].replace('Services:', '').trim() : 'Phone Repair, Screen Replacement';
        const remark = parts[1] ? parts[1].replace('Remark:', '').trim() : 'Good Service';
        
        return { services, remark };
    } catch (error) {
        console.error(`Gemini Error for ${shopName}:`, error.message);
        return { services: 'Phone Repair, Diagnostics', remark: 'Local Pro' };
    }
}

// Main Execution Function
async function processLocations() {
    let locations = [];

    fs.createReadStream(INPUT_FILE)
        .pipe(csv())
        .on('data', (row) => {
            if (row.ZIP && row.CITY) {
                locations.push(row);
            }
        })
        .on('end', async () => {
            // The Hard Stop: 70 Locations
            const targetLocations = locations.slice(0, 70);
            console.log(`Loaded ${locations.length} locations. Trimming to exactly ${targetLocations.length} for this run...`);
            
            for (const loc of targetLocations) {
                const searchQuery = `Phone repair in ${loc.CITY}, ${loc.STATE} ${loc.ZIP}`;
                console.log(`\n🔍 Searching: ${searchQuery}`);
                
                const shops = await getShopsFromSerpApi(searchQuery);
                
                if (shops.length === 0) {
                    console.log('   -> No shops found with 250+ reviews for this location.');
                    continue;
                }

                let savedShopsForThisLocation = 0;
                
                // THE LOCAL BOUNCERS: These completely wipe clean for every new zip code
                const seenBrandsThisZip = new Set(); 
                const seenPhonesThisZip = new Set();

                for (const shop of shops) {
                    if (savedShopsForThisLocation >= 5) break;

                    const name = shop.title || 'N/A';
                    const phone = shop.phone || 'N/A';
                    
                    const normalizedBrand = name.toLowerCase().split(/[-|()]/)[0].trim();

                    // 1. Check Exact Overlap (Prevents saving the exact same store twice for THIS specific zip)
                    if (phone !== 'N/A' && seenPhonesThisZip.has(phone)) {
                        console.log(`   -> ⚠️ Skipped: ${name} (Duplicate listing inside this zip code search)`);
                        continue;
                    }

                    // 2. Check Franchise Overlap (We already saved the best location of this brand for THIS zip)
                    if (seenBrandsThisZip.has(normalizedBrand)) {
                        console.log(`   -> ⚠️ Skipped: ${name} (Lower-rated franchise location in this zip)`);
                        continue;
                    }

                    console.log(`   -> Processing: ${name} (${shop.rating} Stars / ${shop.reviews} Reviews)`);
                    
                    // Add to our local trackers
                    if (phone !== 'N/A') seenPhonesThisZip.add(phone);
                    seenBrandsThisZip.add(normalizedBrand);

                    const rating = shop.rating ? `${shop.rating} (${shop.reviews || 0})` : 'N/A';
                    
                    // Address Fallback
                    let location = shop.address || 'N/A';
                    if (location !== 'N/A' && location.length < 15) {
                        location = `${location}, ${loc.CITY}, ${loc.STATE} ${loc.ZIP}`;
                    }

                    const website = shop.website || 'N/A';
                    
                    const brainWork = await generateShopDetails(name, shop.type, shop.description);
                    
                    const searchLocation = `"${loc.CITY}, ${loc.STATE} ${loc.ZIP}"`;
                    const safeName = `"${name.replace(/"/g, '""')}"`;
                    const safeServices = `"${brainWork.services.replace(/"/g, '""')}"`;
                    const safeRemark = `"${brainWork.remark.replace(/"/g, '""')}"`;
                    const safeLocation = `"${location.replace(/"/g, '""')}"`;
                    
                    const csvRow = `${searchLocation},${safeName},"${rating}",${safeServices},${safeRemark},${safeLocation},"${website}","${phone}"\n`;
                    fs.appendFileSync(OUTPUT_FILE, csvRow);

                    savedShopsForThisLocation++;

                    await new Promise(resolve => setTimeout(resolve, 2500)); 
                }
                
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            console.log('\n✅ Area-Mapping Pipeline complete! Your output file is ready.');
        });
}

processLocations();