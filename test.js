import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
    vus: 1000,          // number of virtual users
    duration: '60s',  // how long to run
};

export default function () {
    http.post('http://localhost:8081/greetRpc?name=ilya');
    //sleep(1);
}
