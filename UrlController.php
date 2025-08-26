<?php

namespace App\Http\Controllers;

use App\Interfaces\UrlControllerInterface;
use App\Models\Url;
use Illuminate\Contracts\Support\Renderable;
use Illuminate\Http\RedirectResponse;
use Illuminate\Http\Request;

class UrlController extends Controller implements UrlControllerInterface
{
    //
    /**
     * Show the application dashboard.
     *
     * @return Renderable
     */
    public function index(): Renderable
    {
        $urlList = Url::all();

        return view('url', compact('urlList'));
    }

    /**
     * @param Request $request
     * @return RedirectResponse
     */
    public function save(Request $request): RedirectResponse
    {
        $key = $request->get('key');
        $value = $request->get('value');
        $id = $request->get('id');

        $allowed_keys = explode(",", env("URL_KEYS"));
		
        if (!in_array(strtolower($key), $allowed_keys, true)){
            return redirect()->back()->with('error', 'URL key is not in allowed URL keys');
        }

        if (!empty($id)) {
            $updated = Url::whereId($id)->update(['url_value' => $value]);
            if (!empty($updated)) {
                $url = Url::whereId($id)->first();
                $key = $url->url_key;
            }
        } else {
            $obj = new Url();
            $obj->url_key = $key;
            $obj->url_value = $value;
            $obj->save();
        }

        $updateController = new UpdateStores();

        $updateController->updateUrls([
            'type' => $key,
            'url' => $value
        ]);

        return redirect()->back()->with('success', 'Record saved');
    }

    /**
     * @param Request $request
     * @return RedirectResponse
     */
    public function delete(Request $request): RedirectResponse
    {
        $id = $request->get('id');
        if (!empty($id)) {
            Url::where('id', $id)->delete();
        }
        return redirect()->back()->with('success', 'Record deleted');
    }
}
