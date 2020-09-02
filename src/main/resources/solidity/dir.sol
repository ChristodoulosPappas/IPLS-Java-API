pragma solidity >=0.4.22 <0.7.0;
//pragma experimental ABIEncoderV2;

/**
 * @title Storage
 * @dev Store & retrieve value in a variable
 */
contract Storage {

    string keyname;
    string[] ProjectNames;
    mapping (string => string) public key;


    //First the client adds the directory of its Project into the
    // IPFS and then stores the hash of the upload
    function store(string memory Pkeyname ,string memory hash) public {
        if(bytes(key[Pkeyname]).length == 0){
            key[Pkeyname] = hash;
            ProjectNames.push(Pkeyname);

        }


    }

   //retrieve hash of the directory on ipfs based on name of project
    function retrieveHash(string memory Pkeyname) public view returns (string memory){
        return key[Pkeyname];
    }


    //Get the names of the projects in order to select one
    function retrieveProjectNames() public view returns(string memory){
        string[] memory str = new string[](ProjectNames.length);
        string memory rstr = "";
        for(uint8 i = 0; i < ProjectNames.length; i++){
            str[i] = ProjectNames[i];
            rstr = string(abi.encodePacked(rstr,str[i]));
            rstr = string(abi.encodePacked(rstr," "));

        }

        return rstr;
    }
    
    
    
    
}