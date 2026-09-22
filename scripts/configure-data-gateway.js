// Configure only after deploying the gateway and installing compatible clients.
const {Client}=require('pg'),crypto=require('node:crypto');
(async()=>{
 require('dotenv').config({path:process.env.RUNWY_ENV_FILE || '.env'});
 const secret=process.env.RUNWY_DATA_GATEWAY_SECRET;
 if(!secret || secret.length<32)throw Error('RUNWY_DATA_GATEWAY_SECRET must contain at least 32 random characters');
 if(!process.argv.includes('--require')&&!process.argv.includes('--prepare'))throw Error('Choose --prepare or --require');
 const db=new Client({connectionString:process.env.DATABASE_URL,ssl:process.env.DATABASE_SSL==='true'?{rejectUnauthorized:process.env.DATABASE_SSL_REJECT_UNAUTHORIZED==='true'}:undefined,connectionTimeoutMillis:10000,statement_timeout:10000});
 await db.connect();try{
 await db.query('update runwy_security.data_gateway_config set secret_hash=$1,required=required or $2 where id',[crypto.createHash('sha256').update(secret).digest('hex'),process.argv.includes('--require')]);
 console.log(JSON.stringify({gatewayRequired:process.argv.includes('--require')}));
 }finally{await db.end()}
})().catch(e=>{console.error(e.message);process.exit(1)});
