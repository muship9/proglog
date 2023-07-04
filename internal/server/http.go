package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
)

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{Log: NewLog()}
}

// ProduceRequest リクエスト側がログに追加して欲しいレコードを格納
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponce どのオフセットにレコードを格納したか
type ProduceResponce struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest リクエスト側が参照したいレコードを指定する
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponce リクエスト側から依頼されたレコードを格納
type ConsumeResponce struct {
	Record Record `json:"record"`
}

// クライアントからのリクエストログを各構造体に追加し、レスポンスで返却します
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	//　リクエストを構造体へアンマーシャルし、ログにレコードを追加
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	//　リクエストを受けたログを Record に格納
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	res := ProduceResponce{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// クライアントからのリクエストログを各構造体に追加し、レスポンスで返却します
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// リクエストをアンマーシャルし、オフセットを取得
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 参照したいオフセットに一致するレコードを取得
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponce{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
