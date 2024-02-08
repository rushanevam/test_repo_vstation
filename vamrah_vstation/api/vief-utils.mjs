//UUID Function 
const suuid = (n=0) => {
    let CHARS = "abcdefghijklmnopqrstuvwxyz0123456789";
    let EXTRA = 2;

    //  Character shuffling if requested by the caller.  This helps in starting the UUIDs from specific character
    let nc = CHARS.length;
    if((n = (n+14)%nc) != 0)
        CHARS = `${CHARS.slice(n)}${CHARS.slice(0, n)}`;

    //  Take current date (in milliseconds) and convert to base-NC
    let dt = Date.now(), nid = "";
    while(dt > 0) {
        nid = CHARS[dt%nc] + nid;
        dt = Math.floor(dt/nc);
    }   
    
    // Append two random characters to avoid collision
    return nid + [...Array(EXTRA).keys()].map(i => CHARS[Math.floor(Math.random() * nc)]).join("");
};

export default { 
    suuid: suuid,
    
    
};