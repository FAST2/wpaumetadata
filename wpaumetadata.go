package wpaumetadata

import (
    "github.com/ncw/swift"
    "encoding/json"
    "bytes"
    //"metadata"
)


type Jobinfo struct {
    JobId string `json:"jobId"`
    Status string `json:"status"`
}


type Jobinfos struct {
    Infos [] Jobinfo `json:"jobs"`
}


func GetMetadata(c swift.Connection, container string) *bytes.Buffer {
    buf := new(bytes.Buffer)
    _, err := c.ObjectGet(container, "jobs.json", buf, true, nil)
    if (err != nil) {
        panic(err)
    }
    return buf
}

func ParseMetadata(buf *bytes.Buffer) (Jobinfos, error) {
    //buf := new(bytes.Buffer)
    infos := Jobinfos{}
    err := json.Unmarshal(buf.Bytes(), &infos)
    if (err != nil) {
        return infos, err
    }
    return infos, nil
}
